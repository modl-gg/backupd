use crate::types::BackupManifest;
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use chrono::{DateTime, Utc};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct StoredManifest {
    pub run_prefix: String,
    pub manifest: BackupManifest,
}

pub async fn list_manifests(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<StoredManifest>> {
    let mut continuation: Option<String> = None;
    let normalized_prefix = format!("{}/", prefix.trim_matches('/'));
    let mut manifest_keys: Vec<String> = Vec::new();

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&normalized_prefix)
            .max_keys(1000);

        if let Some(token) = continuation.clone() {
            request = request.continuation_token(token);
        }

        let page = request
            .send()
            .await
            .context("failed listing manifests from bucket")?;

        manifest_keys.extend(
            page.contents()
                .iter()
                .filter_map(|obj| obj.key())
                .filter(|key| key.ends_with("manifest.json"))
                .map(ToOwned::to_owned),
        );

        if page.is_truncated().unwrap_or(false) {
            continuation = page.next_continuation_token().map(ToOwned::to_owned);
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    let mut manifests = Vec::with_capacity(manifest_keys.len());
    for key in manifest_keys {
        let response = match client
            .get_object()
            .bucket(bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("failed downloading manifest `{key}`"))
        {
            Ok(response) => response,
            Err(error) => {
                warn!(manifest_key = %key, error = %error, "skipping unreadable manifest");
                continue;
            }
        };

        let bytes = match response
            .body
            .collect()
            .await
            .with_context(|| format!("failed reading manifest bytes for `{key}`"))
        {
            Ok(body) => body.into_bytes(),
            Err(error) => {
                warn!(manifest_key = %key, error = %error, "skipping unreadable manifest");
                continue;
            }
        };

        let manifest: BackupManifest = match serde_json::from_slice(&bytes)
            .with_context(|| format!("failed parsing manifest json for `{key}`"))
        {
            Ok(manifest) => manifest,
            Err(error) => {
                warn!(manifest_key = %key, error = %error, "skipping malformed manifest");
                continue;
            }
        };
        let Some(run_prefix) = key.strip_suffix("manifest.json").map(ToOwned::to_owned) else {
            warn!(manifest_key = %key, "skipping manifest with unexpected key shape");
            continue;
        };

        manifests.push(StoredManifest {
            run_prefix,
            manifest,
        });
    }

    manifests.sort_by_key(|m| ReverseDate(m.manifest.completed_at_utc));
    Ok(manifests)
}

pub async fn enforce_retention(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    keep_count: usize,
) -> Result<usize> {
    let manifests = list_manifests(client, bucket, prefix).await?;
    if manifests.len() <= keep_count {
        return Ok(0);
    }

    let stale = manifests.into_iter().skip(keep_count).collect::<Vec<_>>();
    for item in &stale {
        delete_prefix_objects(client, bucket, &item.run_prefix).await?;
    }

    Ok(stale.len())
}

async fn delete_prefix_objects(client: &S3Client, bucket: &str, run_prefix: &str) -> Result<()> {
    let mut continuation: Option<String> = None;
    let mut keys: Vec<String> = Vec::new();

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(run_prefix)
            .max_keys(1000);
        if let Some(token) = continuation.clone() {
            request = request.continuation_token(token);
        }

        let page = request.send().await.with_context(|| {
            format!("failed listing objects for stale run prefix `{run_prefix}`")
        })?;

        keys.extend(
            page.contents()
                .iter()
                .filter_map(|obj| obj.key())
                .map(ToOwned::to_owned),
        );

        if page.is_truncated().unwrap_or(false) {
            continuation = page.next_continuation_token().map(ToOwned::to_owned);
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    for batch in keys.chunks(1000) {
        let objects = batch
            .iter()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                anyhow::anyhow!("failed to build object identifiers for delete: {error}")
            })?;

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(|error| anyhow::anyhow!("failed to build delete request body: {error}"))?;

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .with_context(|| format!("failed deleting stale backup objects in `{run_prefix}`"))?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ReverseDate(DateTime<Utc>);

impl Ord for ReverseDate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for ReverseDate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
