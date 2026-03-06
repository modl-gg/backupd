mod backup;
mod cli;
mod config;
mod notify;
mod restore;
mod scheduler;
mod types;

use crate::backup::BackupService;
use crate::cli::{Cli, Command};
use crate::config::{AppConfig, LogFormat};
use crate::notify::build_notifier;
use crate::restore::RestoreService;
use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(AppConfig::log_format_from_env());
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Run);

    match command {
        Command::Run => {
            let config = Arc::new(AppConfig::from_env()?);
            let notifier = build_notifier(&config);
            let s3 = config.s3_client();
            let backup_service = Arc::new(BackupService::new(config.clone(), s3, notifier));
            backup_service.verify_config(false).await?;
            scheduler::run_scheduler(config, backup_service).await?;
        }
        Command::BackupNow => {
            let config = Arc::new(AppConfig::from_env()?);
            let notifier = build_notifier(&config);
            let s3 = config.s3_client();
            let backup_service = BackupService::new(config, s3, notifier);
            backup_service.verify_config(false).await?;
            let manifest = backup_service.run_backup_once().await?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Command::Restore(args) => {
            let config = Arc::new(AppConfig::from_env()?);
            if !args.drop {
                info!("restore will run without --drop (non-destructive mode)");
            }
            let restore_service = RestoreService::new(config.clone(), config.s3_client());
            restore_service
                .restore_run(
                    &args.run_id,
                    &args.target_uri,
                    args.target_db.as_deref(),
                    args.drop,
                )
                .await?;
        }
        Command::ListBackups => {
            let config = Arc::new(AppConfig::from_env()?);
            let notifier = build_notifier(&config);
            let service = BackupService::new(config.clone(), config.s3_client(), notifier);
            let manifests = service.list_backups().await?;
            println!("{}", serde_json::to_string_pretty(&manifests)?);
        }
        Command::VerifyConfig(args) => {
            let config = Arc::new(AppConfig::from_env()?);
            let notifier = build_notifier(&config);
            let backup_service = BackupService::new(config.clone(), config.s3_client(), notifier);
            backup_service.verify_config(args.skip_remote).await?;
            println!("ok");
        }
    }

    Ok(())
}

fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    match format {
        LogFormat::Json => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        LogFormat::Pretty => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .init(),
    }
}
