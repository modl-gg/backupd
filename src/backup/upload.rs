use crate::backup::encrypt_stream::{EncryptionDescriptor, StreamEncryptor};
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_MULTIPART_PARTS: i32 = 10_000;

#[derive(Debug, Clone)]
pub struct UploadOutcome {
    pub encrypted_size_bytes: u64,
    pub sha256_ciphertext: String,
    pub encryption: EncryptionDescriptor,
}

#[derive(Debug, Clone)]
pub struct UploadSettings<'a> {
    pub passphrase: &'a str,
    pub encryption_chunk_size_bytes: usize,
    pub multipart_part_size_bytes: usize,
    pub max_runtime: Duration,
}

pub async fn stream_encrypt_and_upload<R: AsyncRead + Unpin>(
    client: &S3Client,
    bucket: &str,
    key: &str,
    reader: &mut R,
    settings: UploadSettings<'_>,
) -> Result<UploadOutcome> {
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type("application/octet-stream")
        .send()
        .await
        .context("failed to initiate multipart upload")?;

    let upload_id = create
        .upload_id()
        .context("multipart upload did not return upload_id")?
        .to_owned();

    let operation = async {
        let started_at = tokio::time::Instant::now();
        let encryptor = StreamEncryptor::new(
            settings.passphrase,
            settings.encryption_chunk_size_bytes as u32,
        )?;
        let mut hasher = Sha256::new();
        let mut state =
            MultipartUploadState::new(upload_id.clone(), settings.multipart_part_size_bytes);

        append_hash_and_upload(
            client,
            bucket,
            key,
            &mut state,
            &mut hasher,
            &encryptor.encode_header(),
            started_at,
            settings.max_runtime,
        )
        .await?;

        let mut chunk_index = 0_u64;
        let mut read_buf = vec![0_u8; settings.encryption_chunk_size_bytes];

        loop {
            let read =
                run_with_timeout(started_at, settings.max_runtime, reader.read(&mut read_buf))
                    .await
                    .context("failed reading mongodump output stream")??;
            if read == 0 {
                break;
            }

            let frame = encryptor.encrypt_chunk(chunk_index, &read_buf[..read])?;
            append_hash_and_upload(
                client,
                bucket,
                key,
                &mut state,
                &mut hasher,
                &frame,
                started_at,
                settings.max_runtime,
            )
            .await?;
            chunk_index += 1;
        }

        if !state.upload_buffer.is_empty() || state.completed_parts.is_empty() {
            run_with_timeout(
                started_at,
                settings.max_runtime,
                upload_part(client, bucket, key, &mut state),
            )
            .await??;
        }

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(state.completed_parts))
            .build();

        run_with_timeout(
            started_at,
            settings.max_runtime,
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&state.upload_id)
                .multipart_upload(completed_upload)
                .send(),
        )
        .await
        .context("failed to complete multipart upload")??;

        Ok::<UploadOutcome, anyhow::Error>(UploadOutcome {
            encrypted_size_bytes: state.encrypted_size_bytes,
            sha256_ciphertext: hex::encode(hasher.finalize()),
            encryption: encryptor.descriptor(),
        })
    }
    .await;

    if let Err(error) = &operation {
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        return Err(anyhow::anyhow!("multipart upload aborted: {error}"));
    }

    operation
}

async fn append_hash_and_upload(
    client: &S3Client,
    bucket: &str,
    key: &str,
    state: &mut MultipartUploadState,
    hasher: &mut Sha256,
    bytes: &[u8],
    started_at: tokio::time::Instant,
    max_runtime: Duration,
) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }

    hasher.update(bytes);

    let mut offset = 0usize;
    while offset < bytes.len() {
        let copied = append_into_part_buffer(
            &mut state.upload_buffer,
            state.part_size_bytes,
            &bytes[offset..],
        )?;
        offset += copied;

        if state.upload_buffer.len() == state.part_size_bytes {
            run_with_timeout(
                started_at,
                max_runtime,
                upload_part(client, bucket, key, state),
            )
            .await??;
        }
    }

    Ok(())
}

fn append_into_part_buffer(
    buffer: &mut Vec<u8>,
    part_size_bytes: usize,
    bytes: &[u8],
) -> Result<usize> {
    let Some(remaining_capacity) = part_size_bytes.checked_sub(buffer.len()) else {
        anyhow::bail!(
            "multipart upload buffer exceeded configured part size: buffer_len={}, part_size_bytes={part_size_bytes}",
            buffer.len()
        );
    };

    let to_copy = remaining_capacity.min(bytes.len());
    buffer.extend_from_slice(&bytes[..to_copy]);
    Ok(to_copy)
}

async fn run_with_timeout<T>(
    started_at: tokio::time::Instant,
    max_runtime: Duration,
    future: impl Future<Output = T>,
) -> Result<T> {
    let Some(remaining) = max_runtime.checked_sub(started_at.elapsed()) else {
        anyhow::bail!(
            "backup operation exceeded max runtime of {} seconds",
            max_runtime.as_secs()
        );
    };

    tokio::time::timeout(remaining, future).await.map_err(|_| {
        anyhow::anyhow!(
            "backup operation exceeded max runtime of {} seconds",
            max_runtime.as_secs()
        )
    })
}

async fn upload_part(
    client: &S3Client,
    bucket: &str,
    key: &str,
    state: &mut MultipartUploadState,
) -> Result<()> {
    if state.upload_buffer.is_empty() {
        return Ok(());
    }

    ensure_part_limit_not_exceeded(state.part_number, state.part_size_bytes)?;

    let body_len = state.upload_buffer.len() as u64;
    let payload = std::mem::replace(
        &mut state.upload_buffer,
        Vec::with_capacity(state.part_size_bytes),
    );
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&state.upload_id)
        .part_number(state.part_number)
        .body(ByteStream::from(payload))
        .send()
        .await
        .with_context(|| format!("failed uploading multipart chunk {}", state.part_number))?;

    let etag = response
        .e_tag()
        .context("upload part did not return ETag")?
        .to_owned();

    state.completed_parts.push(
        CompletedPart::builder()
            .part_number(state.part_number)
            .e_tag(etag)
            .build(),
    );
    state.encrypted_size_bytes += body_len;
    state.part_number += 1;
    Ok(())
}

fn ensure_part_limit_not_exceeded(part_number: i32, part_size_bytes: usize) -> Result<()> {
    if part_number <= MAX_MULTIPART_PARTS {
        return Ok(());
    }

    let max_supported_encrypted_size_bytes =
        (part_size_bytes as u64).saturating_mul(MAX_MULTIPART_PARTS as u64);
    anyhow::bail!(
        "multipart upload exceeds S3 limit of {MAX_MULTIPART_PARTS} parts. \
part_size_bytes={part_size_bytes}, max_supported_encrypted_size_bytes={max_supported_encrypted_size_bytes}. \
Increase BACKUP_MULTIPART_PART_SIZE_BYTES."
    );
}

#[derive(Debug)]
struct MultipartUploadState {
    upload_id: String,
    part_number: i32,
    completed_parts: Vec<CompletedPart>,
    encrypted_size_bytes: u64,
    part_size_bytes: usize,
    upload_buffer: Vec<u8>,
}

impl MultipartUploadState {
    fn new(upload_id: String, part_size: usize) -> Self {
        Self {
            upload_id,
            part_number: 1,
            completed_parts: Vec::new(),
            encrypted_size_bytes: 0,
            part_size_bytes: part_size,
            upload_buffer: Vec::with_capacity(part_size),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{append_into_part_buffer, ensure_part_limit_not_exceeded};

    #[test]
    fn allows_upload_up_to_s3_part_limit() {
        ensure_part_limit_not_exceeded(10_000, 8 * 1024 * 1024)
            .expect("part number at limit should pass");
    }

    #[test]
    fn rejects_upload_above_s3_part_limit() {
        let err = ensure_part_limit_not_exceeded(10_001, 8 * 1024 * 1024)
            .expect_err("part number above limit should fail");
        assert!(err.to_string().contains("10000"));
    }

    #[test]
    fn append_into_part_buffer_respects_part_size() {
        let mut buffer = Vec::with_capacity(4);

        let first = append_into_part_buffer(&mut buffer, 4, b"abcdef")
            .expect("first append should succeed");
        assert_eq!(first, 4);
        assert_eq!(buffer, b"abcd");

        let second =
            append_into_part_buffer(&mut buffer, 4, b"xyz").expect("second append should succeed");
        assert_eq!(second, 0);
        assert_eq!(buffer, b"abcd");
    }
}
