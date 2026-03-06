use crate::backup::encrypt_stream::nonce_for_chunk;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 4] = b"MBK1";
const EXPECTED_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct DecryptOutcome {
    pub plaintext_size_bytes: u64,
    pub sha256_ciphertext: String,
}

pub async fn decrypt_stream_to_writer<R, W>(
    reader: &mut R,
    writer: &mut W,
    passphrase: &str,
) -> Result<DecryptOutcome>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut hasher = Sha256::new();

    let mut magic = [0_u8; 4];
    read_exact_and_hash(reader, &mut hasher, &mut magic).await?;
    if &magic != MAGIC {
        anyhow::bail!("backup stream has invalid magic header");
    }

    let mut version = [0_u8; 1];
    read_exact_and_hash(reader, &mut hasher, &mut version).await?;
    if version[0] != EXPECTED_FORMAT_VERSION {
        anyhow::bail!("unsupported backup stream format version {}", version[0]);
    }

    let chunk_size = read_u32_and_hash(reader, &mut hasher).await?;
    if chunk_size == 0 {
        anyhow::bail!("encrypted stream chunk size cannot be zero");
    }
    let memory_kib = read_u32_and_hash(reader, &mut hasher).await?;
    let iterations = read_u32_and_hash(reader, &mut hasher).await?;
    let parallelism = read_u32_and_hash(reader, &mut hasher).await?;

    let mut salt_len = [0_u8; 1];
    let mut nonce_len = [0_u8; 1];
    read_exact_and_hash(reader, &mut hasher, &mut salt_len).await?;
    read_exact_and_hash(reader, &mut hasher, &mut nonce_len).await?;

    let salt_len = salt_len[0] as usize;
    let nonce_len = nonce_len[0] as usize;

    if salt_len != 16 || nonce_len != 12 {
        anyhow::bail!("unsupported stream salt/nonce length");
    }

    let mut salt = vec![0_u8; salt_len];
    read_exact_and_hash(reader, &mut hasher, &mut salt).await?;

    let mut base_nonce = [0_u8; 12];
    read_exact_and_hash(reader, &mut hasher, &mut base_nonce).await?;

    let params = Params::new(memory_kib, iterations, parallelism, Some(32))
        .map_err(|error| anyhow::anyhow!("invalid argon2 parameters in backup header: {error}"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0_u8; 32];
    argon
        .hash_password_into(passphrase.as_bytes(), &salt, &mut key)
        .map_err(|error| anyhow::anyhow!("failed to derive decryption key: {error}"))?;

    let cipher = Aes256Gcm::new_from_slice(&key).context("failed to create decryption cipher")?;
    let max_frame_len = chunk_size as usize + 16;

    let mut chunk_index = 0_u64;
    let mut plaintext_size_bytes = 0_u64;

    loop {
        let frame_len_bytes = match read_frame_length(reader).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => break,
            Err(error) => return Err(error).context("failed reading encrypted frame length"),
        };
        hasher.update(frame_len_bytes);

        let frame_len = u32::from_le_bytes(frame_len_bytes) as usize;
        if frame_len == 0 {
            anyhow::bail!("encrypted frame length cannot be zero");
        }
        if frame_len > max_frame_len {
            anyhow::bail!("encrypted frame length is larger than configured chunk limit");
        }

        let mut frame = vec![0_u8; frame_len];
        read_exact_and_hash(reader, &mut hasher, &mut frame).await?;

        let aad = chunk_index.to_le_bytes();
        let nonce = nonce_for_chunk(&base_nonce, chunk_index);
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &frame,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("decryption failed for chunk {chunk_index}"))?;

        writer
            .write_all(&plaintext)
            .await
            .context("failed writing decrypted payload to restore stream")?;
        plaintext_size_bytes += plaintext.len() as u64;
        chunk_index += 1;
    }

    writer
        .flush()
        .await
        .context("failed to flush restore writer")?;
    Ok(DecryptOutcome {
        plaintext_size_bytes,
        sha256_ciphertext: hex::encode(hasher.finalize()),
    })
}

async fn read_frame_length<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<[u8; 4]>> {
    let mut first = [0_u8; 1];
    let first_read = reader.read(&mut first).await?;
    if first_read == 0 {
        return Ok(None);
    }

    let mut rest = [0_u8; 3];
    reader
        .read_exact(&mut rest)
        .await
        .context("truncated encrypted frame length")?;

    Ok(Some([first[0], rest[0], rest[1], rest[2]]))
}

async fn read_u32_and_hash<R: AsyncRead + Unpin>(
    reader: &mut R,
    hasher: &mut Sha256,
) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    read_exact_and_hash(reader, hasher, &mut bytes).await?;
    Ok(u32::from_le_bytes(bytes))
}

async fn read_exact_and_hash<R: AsyncRead + Unpin>(
    reader: &mut R,
    hasher: &mut Sha256,
    buf: &mut [u8],
) -> Result<()> {
    reader.read_exact(buf).await?;
    hasher.update(buf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::encrypt_stream::StreamEncryptor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn decrypts_stream_created_by_encryptor() {
        let plaintext = b"hello world this is a backup payload";
        let encryptor =
            StreamEncryptor::new("very-secure-passphrase-123", 1024).expect("encryptor");
        let mut stream = encryptor.encode_header();
        stream.extend_from_slice(
            &encryptor
                .encrypt_chunk(0, plaintext)
                .expect("chunk should encrypt"),
        );

        let mut input = BufReader::new(std::io::Cursor::new(stream));
        let mut output = Vec::new();
        let outcome =
            decrypt_stream_to_writer(&mut input, &mut output, "very-secure-passphrase-123")
                .await
                .expect("decrypt should succeed");

        assert_eq!(output, plaintext);
        assert!(outcome.plaintext_size_bytes > 0);
    }

    #[tokio::test]
    async fn fails_when_frame_length_is_truncated() {
        let encryptor =
            StreamEncryptor::new("very-secure-passphrase-123", 1024).expect("encryptor");
        let mut stream = encryptor.encode_header();
        let frame = encryptor
            .encrypt_chunk(0, b"abc")
            .expect("chunk should encrypt");
        stream.extend_from_slice(&frame[..2]);

        let mut input = BufReader::new(std::io::Cursor::new(stream));
        let mut output = Vec::new();
        let error = decrypt_stream_to_writer(&mut input, &mut output, "very-secure-passphrase-123")
            .await
            .expect_err("truncated frame length must fail");

        assert!(
            error
                .to_string()
                .contains("failed reading encrypted frame length")
        );
    }
}
