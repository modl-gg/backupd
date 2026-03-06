pub mod encrypt_stream;
pub mod mongodump;
pub mod retention;
pub mod upload;

use crate::config::{AppConfig, OplogMode};
use crate::notify::{BackupFailureEvent, SharedNotifier};
use crate::types::{
    BackupManifest, BackupStatus, ManifestArgon2Params, ManifestEncryption, new_backup_object_keys,
};
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use chrono::Utc;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

pub struct BackupService {
    config: Arc<AppConfig>,
    s3: S3Client,
    notifier: SharedNotifier,
    run_lock: Mutex<()>,
}

impl BackupService {
    pub fn new(config: Arc<AppConfig>, s3: S3Client, notifier: SharedNotifier) -> Self {
        Self {
            config,
            s3,
            notifier,
            run_lock: Mutex::new(()),
        }
    }

    pub async fn verify_config(&self, skip_remote: bool) -> Result<()> {
        mongodump::verify_binary("mongodump").await?;
        mongodump::verify_binary("mongorestore").await?;

        if !skip_remote {
            self.s3
                .head_bucket()
                .bucket(&self.config.backblaze.bucket_name)
                .send()
                .await
                .context("failed to access configured Backblaze bucket")?;
        }

        Ok(())
    }

    pub async fn list_backups(&self) -> Result<Vec<BackupManifest>> {
        let manifests = retention::list_manifests(
            &self.s3,
            &self.config.backblaze.bucket_name,
            &self.config.backup_prefix,
        )
        .await?;

        Ok(manifests
            .into_iter()
            .map(|stored| stored.manifest)
            .collect())
    }

    pub async fn run_backup_once(&self) -> Result<BackupManifest> {
        let _run_guard = self.run_lock.lock().await;
        let started_at = Utc::now();
        let keys = new_backup_object_keys(&self.config.backup_prefix, started_at);
        let run_id = keys.run_id.clone();

        match self.run_backup_with_keys(started_at, keys).await {
            Ok(manifest) => {
                info!(
                    run_id = %manifest.run_id,
                    encrypted_size_bytes = manifest.encrypted_size_bytes,
                    "backup run completed successfully"
                );
                Ok(manifest)
            }
            Err(stage_error) => {
                self.notify_failure(&run_id, &stage_error.stage, &stage_error.message)
                    .await;
                Err(anyhow::anyhow!(stage_error))
            }
        }
    }

    async fn run_backup_with_keys(
        &self,
        started_at: chrono::DateTime<Utc>,
        keys: crate::types::BackupObjectKeys,
    ) -> std::result::Result<BackupManifest, BackupStageError> {
        let run_id = keys.run_id.clone();
        info!(run_id = %run_id, "backup run started");

        let (upload_outcome, oplog_used) = match self.config.backup_oplog_mode {
            OplogMode::Off => {
                self.execute_dump_and_upload(false, &keys.archive_key)
                    .await?
            }
            OplogMode::Required => {
                self.execute_dump_and_upload(true, &keys.archive_key)
                    .await?
            }
            OplogMode::Auto => match self.execute_dump_and_upload(true, &keys.archive_key).await {
                Ok(outcome) => outcome,
                Err(error) if is_oplog_unsupported(&error.message) => {
                    warn!(
                        run_id = %run_id,
                        message = %error.message,
                        "oplog mode is unavailable, retrying backup without --oplog"
                    );
                    self.execute_dump_and_upload(false, &keys.archive_key)
                        .await?
                }
                Err(error) => return Err(error),
            },
        };

        let completed_at = Utc::now();
        let manifest = BackupManifest {
            run_id: run_id.clone(),
            started_at_utc: started_at,
            completed_at_utc: completed_at,
            source_uri_fingerprint: self.config.source_uri_fingerprint(),
            oplog_used,
            archive_format: "mongodump-archive-gzip".to_owned(),
            encryption: ManifestEncryption {
                algorithm: "aes-256-gcm-chunked-v1".to_owned(),
                format_version: upload_outcome.encryption.format_version,
                chunk_size_bytes: upload_outcome.encryption.chunk_size_bytes,
                base_nonce_hex: upload_outcome.encryption.base_nonce_hex,
            },
            kdf: "argon2id".to_owned(),
            kdf_params: ManifestArgon2Params {
                memory_kib: upload_outcome.encryption.argon2_memory_kib,
                iterations: upload_outcome.encryption.argon2_iterations,
                parallelism: upload_outcome.encryption.argon2_parallelism,
                salt_hex: upload_outcome.encryption.salt_hex,
            },
            encrypted_size_bytes: upload_outcome.encrypted_size_bytes,
            sha256_ciphertext: upload_outcome.sha256_ciphertext,
            backup_prefix: self.config.backup_prefix.clone(),
            bucket: self.config.backblaze.bucket_name.clone(),
            status: BackupStatus::Success,
        };

        if let Err(error) = self.store_manifest(&manifest, &keys.manifest_key).await {
            self.cleanup_archive_object(&keys.archive_key).await;
            return Err(error);
        }

        let deleted = match retention::enforce_retention(
            &self.s3,
            &self.config.backblaze.bucket_name,
            &self.config.backup_prefix,
            self.config.backup_retention_count,
        )
        .await
        {
            Ok(deleted) => deleted,
            Err(error) => {
                let message = format!("retention failed: {error}");
                error!(run_id = %run_id, error = %message, "retention enforcement failed");
                self.notify_failure(&run_id, "retention", &message).await;
                0
            }
        };

        info!(
            run_id = %run_id,
            deleted_runs = deleted,
            archive_key = %keys.archive_key,
            "backup run finalized"
        );

        Ok(manifest)
    }

    async fn store_manifest(
        &self,
        manifest: &BackupManifest,
        manifest_key: &str,
    ) -> std::result::Result<(), BackupStageError> {
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| BackupStageError::new("manifest_serialize", error.to_string()))?;
        self.s3
            .put_object()
            .bucket(&self.config.backblaze.bucket_name)
            .key(manifest_key)
            .content_type("application/json")
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|error| BackupStageError::new("manifest_upload", error.to_string()))?;
        Ok(())
    }

    async fn execute_dump_and_upload(
        &self,
        use_oplog: bool,
        archive_key: &str,
    ) -> std::result::Result<(upload::UploadOutcome, bool), BackupStageError> {
        let mut dump = mongodump::spawn_mongodump(&self.config.mongodb_uri, use_oplog)
            .map_err(|error| BackupStageError::new("dump_start", error.to_string()))?;

        let upload_result = upload::stream_encrypt_and_upload(
            &self.s3,
            &self.config.backblaze.bucket_name,
            archive_key,
            &mut dump.stdout,
            upload::UploadSettings {
                passphrase: &self.config.backup_encryption_passphrase,
                encryption_chunk_size_bytes: self.config.encryption_chunk_size_bytes,
                multipart_part_size_bytes: self.config.multipart_part_size_bytes,
                max_runtime: self.config.max_runtime(),
            },
        )
        .await;

        let upload = match upload_result {
            Ok(upload) => upload,
            Err(error) => {
                error!(error = %error, "upload path failed; killing mongodump");
                let _ = dump.kill().await;
                let _ = dump.wait().await;
                return Err(BackupStageError::new("upload", error.to_string()));
            }
        };

        let exit = match dump.wait().await {
            Ok(exit) => exit,
            Err(error) => {
                self.cleanup_archive_object(archive_key).await;
                return Err(BackupStageError::new("dump_wait", error.to_string()));
            }
        };

        if !exit.success {
            self.cleanup_archive_object(archive_key).await;
            return Err(BackupStageError::new(
                "dump",
                format!("mongodump exited non-zero: {}", exit.stderr),
            ));
        }

        Ok((upload, use_oplog))
    }

    async fn cleanup_archive_object(&self, archive_key: &str) {
        let result = self
            .s3
            .delete_object()
            .bucket(&self.config.backblaze.bucket_name)
            .key(archive_key)
            .send()
            .await;

        if let Err(error) = result {
            warn!(
                archive_key = archive_key,
                error = %error,
                "failed to cleanup archive object after failed backup run"
            );
        }
    }

    async fn notify_failure(&self, run_id: &str, stage: &str, message: &str) {
        self.notifier
            .notify_backup_failure(&BackupFailureEvent {
                run_id: run_id.to_owned(),
                stage: stage.to_owned(),
                error: message.to_owned(),
                occurred_at_utc: Utc::now(),
            })
            .await;
    }
}

#[derive(Debug, Error)]
#[error("backup failed at stage `{stage}`: {message}")]
struct BackupStageError {
    stage: String,
    message: String,
}

impl BackupStageError {
    fn new(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            message: message.into(),
        }
    }
}

fn is_oplog_unsupported(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("oplog")
        && (lower.contains("replset")
            || lower.contains("replica set")
            || lower.contains("not supported")
            || lower.contains("can only be used against a mongod started as a replica set member"))
}
