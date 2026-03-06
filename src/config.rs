use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::env;
use std::time::Duration;
use url::Url;

const DEFAULT_B2_ENDPOINT: &str = "https://s3.us-east-005.backblazeb2.com";
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_BUCKET: &str = "modl-database-backups";
const DEFAULT_PREFIX: &str = "mongo-backups";
const DEFAULT_INTERVAL_SECONDS: u64 = 10_800;
const DEFAULT_RETENTION_COUNT: usize = 12;
const DEFAULT_MAX_RUNTIME_SECONDS: u64 = 7_200;
const DEFAULT_PART_SIZE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OplogMode {
    Auto,
    Off,
    Required,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone)]
pub struct BackblazeConfig {
    pub key_id: String,
    pub application_key: String,
    pub endpoint: String,
    pub region: String,
    pub bucket_name: String,
}

#[derive(Debug, Clone)]
pub struct AlertConfig {
    pub discord_webhook_url: Option<String>,
    pub discord_role_mention: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub mongodb_uri: String,
    pub backblaze: BackblazeConfig,
    pub backup_prefix: String,
    pub backup_interval_seconds: u64,
    pub backup_retention_count: usize,
    pub backup_encryption_passphrase: String,
    pub backup_oplog_mode: OplogMode,
    pub backup_max_runtime_seconds: u64,
    pub multipart_part_size_bytes: usize,
    pub encryption_chunk_size_bytes: usize,
    pub backup_run_on_start: bool,
    pub alerting: AlertConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let mongodb_uri = required("MONGODB_URI")?;
        let key_id = required("BACKBLAZE_KEY_ID")?;
        let application_key = required("BACKBLAZE_APPLICATION_KEY")?;
        let endpoint =
            optional("BACKBLAZE_ENDPOINT").unwrap_or_else(|| DEFAULT_B2_ENDPOINT.to_owned());
        let region = optional("BACKBLAZE_REGION").unwrap_or_else(|| {
            derive_s3_region_from_endpoint(&endpoint)
                .unwrap_or_else(|| DEFAULT_S3_REGION.to_owned())
        });
        let bucket_name =
            optional("BACKBLAZE_BUCKET_NAME").unwrap_or_else(|| DEFAULT_BUCKET.to_owned());
        let backup_prefix = sanitize_prefix(
            &optional("BACKUP_PREFIX").unwrap_or_else(|| DEFAULT_PREFIX.to_owned()),
        );

        let backup_interval_seconds =
            parse_u64("BACKUP_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS)?;
        let backup_retention_count =
            parse_usize("BACKUP_RETENTION_COUNT", DEFAULT_RETENTION_COUNT)?;
        let backup_encryption_passphrase = required("BACKUP_ENCRYPTION_PASSPHRASE")?;
        let backup_max_runtime_seconds =
            parse_u64("BACKUP_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS)?;
        let multipart_part_size_bytes =
            parse_usize("BACKUP_MULTIPART_PART_SIZE_BYTES", DEFAULT_PART_SIZE_BYTES)?;
        let encryption_chunk_size_bytes = parse_usize(
            "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES",
            DEFAULT_CHUNK_SIZE_BYTES,
        )?;
        let backup_run_on_start = parse_bool("BACKUP_RUN_ON_START", false)?;
        let backup_oplog_mode = parse_oplog_mode(optional("BACKUP_OPLOG_MODE").as_deref())?;

        let discord_webhook_url = optional("DISCORD_WEBHOOK_URL");
        let discord_role_mention = optional("DISCORD_ROLE_MENTION");

        let config = Self {
            mongodb_uri,
            backblaze: BackblazeConfig {
                key_id,
                application_key,
                endpoint,
                region,
                bucket_name,
            },
            backup_prefix,
            backup_interval_seconds,
            backup_retention_count,
            backup_encryption_passphrase,
            backup_oplog_mode,
            backup_max_runtime_seconds,
            multipart_part_size_bytes,
            encryption_chunk_size_bytes,
            backup_run_on_start,
            alerting: AlertConfig {
                discord_webhook_url,
                discord_role_mention,
            },
        };

        config.validate()?;
        Ok(config)
    }

    pub fn log_format_from_env() -> LogFormat {
        match optional("BACKUP_LOG_FORMAT")
            .unwrap_or_else(|| "json".to_owned())
            .to_ascii_lowercase()
            .as_str()
        {
            "pretty" => LogFormat::Pretty,
            _ => LogFormat::Json,
        }
    }

    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.backup_interval_seconds)
    }

    pub fn max_runtime(&self) -> Duration {
        Duration::from_secs(self.backup_max_runtime_seconds)
    }

    pub fn source_uri_fingerprint(&self) -> String {
        let normalized = normalize_mongo_uri(&self.mongodb_uri);
        let mut hasher = Sha256::new();
        hasher.update(normalized.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn s3_client(&self) -> S3Client {
        let credentials = Credentials::new(
            self.backblaze.key_id.clone(),
            self.backblaze.application_key.clone(),
            None,
            None,
            "backblaze-static",
        );

        let conf = aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials)
            .region(Region::new(self.backblaze.region.clone()))
            .endpoint_url(self.backblaze.endpoint.clone())
            .force_path_style(true)
            .behavior_version_latest()
            .build();

        S3Client::from_conf(conf)
    }

    fn validate(&self) -> Result<()> {
        if self.backup_prefix.is_empty() {
            anyhow::bail!("BACKUP_PREFIX cannot resolve to an empty prefix");
        }
        if self.backblaze.region.trim().is_empty() {
            anyhow::bail!("BACKBLAZE_REGION cannot be empty");
        }
        if self.backup_interval_seconds == 0 {
            anyhow::bail!("BACKUP_INTERVAL_SECONDS must be greater than zero");
        }
        if self.backup_retention_count == 0 {
            anyhow::bail!("BACKUP_RETENTION_COUNT must be greater than zero");
        }
        if self.backup_encryption_passphrase.len() < 16 {
            anyhow::bail!("BACKUP_ENCRYPTION_PASSPHRASE must be at least 16 characters");
        }
        if self.multipart_part_size_bytes < 5 * 1024 * 1024 {
            anyhow::bail!("BACKUP_MULTIPART_PART_SIZE_BYTES must be at least 5 MiB");
        }
        if self.encryption_chunk_size_bytes == 0 {
            anyhow::bail!("BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES must be greater than zero");
        }
        if self.encryption_chunk_size_bytes > self.multipart_part_size_bytes {
            anyhow::bail!(
                "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES must be <= BACKUP_MULTIPART_PART_SIZE_BYTES"
            );
        }
        if self.backup_max_runtime_seconds == 0 {
            anyhow::bail!("BACKUP_MAX_RUNTIME_SECONDS must be greater than zero");
        }
        Ok(())
    }
}

fn normalize_mongo_uri(uri: &str) -> String {
    if let Ok(mut parsed) = Url::parse(uri) {
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        return parsed.to_string();
    }
    uri.to_owned()
}

fn required(key: &str) -> Result<String> {
    optional(key).with_context(|| format!("{key} is required"))
}

fn optional(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn parse_bool(key: &str, default: bool) -> Result<bool> {
    let Some(raw) = optional(key) else {
        return Ok(default);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{key} must be a boolean value"),
    }
}

fn parse_u64(key: &str, default: u64) -> Result<u64> {
    let Some(raw) = optional(key) else {
        return Ok(default);
    };
    raw.parse::<u64>()
        .with_context(|| format!("{key} must be a valid positive integer"))
}

fn parse_usize(key: &str, default: usize) -> Result<usize> {
    let Some(raw) = optional(key) else {
        return Ok(default);
    };
    raw.parse::<usize>()
        .with_context(|| format!("{key} must be a valid positive integer"))
}

fn parse_oplog_mode(raw: Option<&str>) -> Result<OplogMode> {
    match raw.unwrap_or("auto").to_ascii_lowercase().as_str() {
        "auto" => Ok(OplogMode::Auto),
        "off" => Ok(OplogMode::Off),
        "required" => Ok(OplogMode::Required),
        _ => anyhow::bail!("BACKUP_OPLOG_MODE must be one of: auto, off, required"),
    }
}

fn sanitize_prefix(raw: &str) -> String {
    raw.trim_matches('/').to_owned()
}

fn derive_s3_region_from_endpoint(endpoint: &str) -> Option<String> {
    let parsed = Url::parse(endpoint).ok()?;
    let host = parsed.host_str()?;
    let labels: Vec<&str> = host.split('.').collect();

    if labels.is_empty() {
        return None;
    }

    if labels[0].eq_ignore_ascii_case("s3") {
        if labels.get(1)?.eq_ignore_ascii_case("dualstack") {
            return labels
                .get(2)
                .and_then(|value| (!value.is_empty()).then(|| (*value).to_owned()));
        }
        return labels
            .get(1)
            .and_then(|value| (!value.is_empty()).then(|| (*value).to_owned()));
    }

    if let Some(region) = labels[0].strip_prefix("s3-")
        && !region.is_empty()
    {
        return Some(region.to_owned());
    }

    if let Some(index) = labels
        .iter()
        .position(|label| label.eq_ignore_ascii_case("s3"))
    {
        if labels.get(index + 1)?.eq_ignore_ascii_case("dualstack") {
            return labels
                .get(index + 2)
                .and_then(|value| (!value.is_empty()).then(|| (*value).to_owned()));
        }
        return labels
            .get(index + 1)
            .and_then(|value| (!value.is_empty()).then(|| (*value).to_owned()));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_sanitization_removes_edge_slashes() {
        assert_eq!(sanitize_prefix("/mongo-backups/"), "mongo-backups");
        assert_eq!(sanitize_prefix("mongo-backups"), "mongo-backups");
    }

    #[test]
    fn parse_oplog_mode_defaults_to_auto() {
        let mode = parse_oplog_mode(None).expect("mode should parse");
        assert_eq!(mode, OplogMode::Auto);
    }

    #[test]
    fn derives_region_from_default_backblaze_endpoint() {
        let region =
            derive_s3_region_from_endpoint(DEFAULT_B2_ENDPOINT).expect("region should parse");
        assert_eq!(region, "us-east-005");
    }

    #[test]
    fn derives_region_from_bucket_style_endpoint() {
        let region = derive_s3_region_from_endpoint("https://my-bucket.s3.us-west-2.amazonaws.com")
            .expect("region should parse");
        assert_eq!(region, "us-west-2");
    }

    #[test]
    fn derive_region_returns_none_for_non_s3_hostname() {
        let region = derive_s3_region_from_endpoint("http://localhost:9000");
        assert_eq!(region, None);
    }
}
