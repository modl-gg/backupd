use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    Success,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestArgon2Params {
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
    pub salt_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEncryption {
    pub algorithm: String,
    pub format_version: u8,
    pub chunk_size_bytes: u32,
    pub base_nonce_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub run_id: String,
    pub started_at_utc: DateTime<Utc>,
    pub completed_at_utc: DateTime<Utc>,
    pub source_uri_fingerprint: String,
    pub oplog_used: bool,
    pub archive_format: String,
    pub encryption: ManifestEncryption,
    pub kdf: String,
    pub kdf_params: ManifestArgon2Params,
    pub encrypted_size_bytes: u64,
    pub sha256_ciphertext: String,
    pub backup_prefix: String,
    pub bucket: String,
    pub status: BackupStatus,
}

#[derive(Debug, Clone)]
pub struct BackupObjectKeys {
    pub run_id: String,
    pub archive_key: String,
    pub manifest_key: String,
}

impl BackupObjectKeys {
    pub fn from_run_id(prefix: &str, run_id: &str) -> Self {
        let (date_prefix, normalized_run_id) = run_id_to_prefix(run_id);
        let run_prefix = format!(
            "{}/{}/{}/",
            prefix.trim_matches('/'),
            date_prefix,
            normalized_run_id
        );
        let archive_key = format!("{run_prefix}dump.archive.gz.enc");
        let manifest_key = format!("{run_prefix}manifest.json");

        Self {
            run_id: normalized_run_id,
            archive_key,
            manifest_key,
        }
    }
}

pub fn new_backup_object_keys(prefix: &str, timestamp: DateTime<Utc>) -> BackupObjectKeys {
    let short_uuid = Uuid::new_v4().simple().to_string()[..8].to_owned();
    let run_id = format!("{}-{short_uuid}", timestamp.format("%Y%m%dT%H%M%SZ"));
    let date_prefix = format!(
        "{:04}/{:02}/{:02}",
        timestamp.year(),
        timestamp.month(),
        timestamp.day()
    );
    let run_prefix = format!("{}/{}/{}/", prefix.trim_matches('/'), date_prefix, run_id);

    BackupObjectKeys {
        run_id,
        archive_key: format!("{run_prefix}dump.archive.gz.enc"),
        manifest_key: format!("{run_prefix}manifest.json"),
    }
}

fn run_id_to_prefix(run_id: &str) -> (String, String) {
    let normalized_run_id = run_id.trim().to_owned();
    let timestamp = normalized_run_id
        .split('-')
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();

    if timestamp.len() >= 8 {
        let year = &timestamp[0..4];
        let month = &timestamp[4..6];
        let day = &timestamp[6..8];
        (format!("{year}/{month}/{day}"), normalized_run_id)
    } else {
        ("unknown/unknown/unknown".to_owned(), normalized_run_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn generates_expected_prefix_shape() {
        let dt = Utc.with_ymd_and_hms(2026, 3, 4, 7, 8, 9).unwrap();
        let keys = new_backup_object_keys("mongo-backups", dt);
        assert!(
            keys.archive_key
                .starts_with("mongo-backups/2026/03/04/20260304T070809Z-")
        );
        assert!(keys.archive_key.ends_with("dump.archive.gz.enc"));
        assert!(keys.manifest_key.ends_with("manifest.json"));
    }
}
