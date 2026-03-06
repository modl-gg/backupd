use crate::types::BackupManifest;
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;

pub async fn fetch_manifest(
    client: &S3Client,
    bucket: &str,
    manifest_key: &str,
) -> Result<BackupManifest> {
    let bytes = client
        .get_object()
        .bucket(bucket)
        .key(manifest_key)
        .send()
        .await
        .with_context(|| format!("failed to download manifest `{manifest_key}`"))?
        .body
        .collect()
        .await
        .with_context(|| format!("failed to read manifest body `{manifest_key}`"))?;

    let manifest: BackupManifest = serde_json::from_slice(&bytes.into_bytes())
        .with_context(|| format!("manifest json is invalid for key `{manifest_key}`"))?;
    Ok(manifest)
}

pub async fn fetch_object_stream(client: &S3Client, bucket: &str, key: &str) -> Result<ByteStream> {
    let response = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("failed to download backup object `{key}`"))?;
    Ok(response.body)
}
