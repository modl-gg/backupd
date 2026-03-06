use crate::backup::BackupService;
use crate::config::AppConfig;
use anyhow::Result;
use std::sync::Arc;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

pub async fn run_scheduler(config: Arc<AppConfig>, service: Arc<BackupService>) -> Result<()> {
    info!(
        interval_seconds = config.backup_interval_seconds,
        retention_count = config.backup_retention_count,
        "scheduler started"
    );

    if config.backup_run_on_start {
        info!("running startup backup");
        if let Err(error) = service.run_backup_once().await {
            error!(error = %error, "startup backup failed");
        }
    }

    let mut interval = tokio::time::interval(config.interval());
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
            _ = interval.tick() => {
                if let Err(error) = service.run_backup_once().await {
                    error!(error = %error, "scheduled backup failed");
                }
            }
        }
    }

    Ok(())
}
