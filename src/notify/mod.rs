pub mod discord;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use crate::config::AppConfig;

#[derive(Debug, Clone)]
pub struct BackupFailureEvent {
    pub run_id: String,
    pub stage: String,
    pub error: String,
    pub occurred_at_utc: DateTime<Utc>,
}

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn notify_backup_failure(&self, event: &BackupFailureEvent);
}

pub type SharedNotifier = Arc<dyn Notifier>;

pub struct NoopNotifier;

#[async_trait]
impl Notifier for NoopNotifier {
    async fn notify_backup_failure(&self, _event: &BackupFailureEvent) {}
}

pub fn build_notifier(config: &AppConfig) -> SharedNotifier {
    if let Some(url) = config.alerting.discord_webhook_url.clone() {
        Arc::new(discord::DiscordNotifier::new(
            url,
            config.alerting.discord_role_mention.clone(),
        ))
    } else {
        Arc::new(NoopNotifier)
    }
}
