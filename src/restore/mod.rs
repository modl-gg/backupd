pub mod decrypt_stream;
pub mod download;

use crate::config::AppConfig;
use crate::types::BackupObjectKeys;
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::info;

pub struct RestoreService {
    config: Arc<AppConfig>,
    s3: S3Client,
}

impl RestoreService {
    pub fn new(config: Arc<AppConfig>, s3: S3Client) -> Self {
        Self { config, s3 }
    }

    pub async fn restore_run(
        &self,
        run_id: &str,
        target_uri: &str,
        target_db: Option<&str>,
        drop_collections: bool,
    ) -> Result<()> {
        let keys = BackupObjectKeys::from_run_id(&self.config.backup_prefix, run_id);

        let manifest = download::fetch_manifest(
            &self.s3,
            &self.config.backblaze.bucket_name,
            &keys.manifest_key,
        )
        .await?;

        ensure_manifest_run_id_matches_request(&manifest.run_id, &keys.run_id)?;

        let stream = download::fetch_object_stream(
            &self.s3,
            &self.config.backblaze.bucket_name,
            &keys.archive_key,
        )
        .await?;
        let mut encrypted_reader = stream.into_async_read();

        let mut cmd = Command::new("mongorestore");
        cmd.arg("--uri")
            .arg(target_uri)
            .arg("--archive")
            .arg("--gzip");

        if drop_collections {
            cmd.arg("--drop");
        }
        if let Some(db) = target_db {
            cmd.arg("--nsInclude").arg(format!("{db}.*"));
        }

        cmd.stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .context("failed starting mongorestore process")?;
        let mut stdin = child
            .stdin
            .take()
            .context("mongorestore stdin unavailable")?;

        let mut stderr = child
            .stderr
            .take()
            .context("mongorestore stderr unavailable")?;

        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await?;
            Ok::<String, std::io::Error>(String::from_utf8_lossy(&bytes).trim().to_owned())
        });

        let decrypt_result = decrypt_stream::decrypt_stream_to_writer(
            &mut encrypted_reader,
            &mut stdin,
            &self.config.backup_encryption_passphrase,
        )
        .await;

        if let Err(error) = decrypt_result {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error).context("failed while decrypting restore stream");
        }

        let decrypt_outcome = decrypt_result?;
        drop(stdin);

        if decrypt_outcome.sha256_ciphertext != manifest.sha256_ciphertext {
            let _ = child.kill().await;
            let _ = child.wait().await;
            anyhow::bail!("encrypted stream checksum mismatch; restore aborted");
        }

        let status = child
            .wait()
            .await
            .context("failed waiting for mongorestore")?;
        let stderr = stderr_task.await.context("failed joining stderr task")??;

        if !status.success() {
            anyhow::bail!("mongorestore failed: {stderr}");
        }

        info!(
            run_id = %manifest.run_id,
            target_db = ?target_db,
            plaintext_size_bytes = decrypt_outcome.plaintext_size_bytes,
            "restore completed successfully"
        );
        Ok(())
    }
}

fn ensure_manifest_run_id_matches_request(
    manifest_run_id: &str,
    requested_run_id: &str,
) -> Result<()> {
    if manifest_run_id == requested_run_id {
        return Ok(());
    }

    anyhow::bail!(
        "manifest run id mismatch: requested `{requested_run_id}`, found `{manifest_run_id}`; restore aborted"
    );
}

#[cfg(test)]
mod tests {
    use super::ensure_manifest_run_id_matches_request;

    #[test]
    fn allows_matching_manifest_run_id() {
        ensure_manifest_run_id_matches_request(
            "20260305T120000Z-abcd1234",
            "20260305T120000Z-abcd1234",
        )
        .expect("matching run ids should pass");
    }

    #[test]
    fn rejects_mismatched_manifest_run_id() {
        let err = ensure_manifest_run_id_matches_request(
            "20260305T120000Z-abcd1234",
            "20260305T130000Z-efgh5678",
        )
        .expect_err("mismatched run ids should fail");

        assert!(err.to_string().contains("manifest run id mismatch"));
    }
}
