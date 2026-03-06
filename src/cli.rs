use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "backupd",
    version,
    about = "Streamed backup service for MongoDB and S3 buckets with compression/encryption support"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run long-lived scheduler mode
    Run,
    /// Run one backup immediately, then exit
    BackupNow,
    /// Restore a backup run into a MongoDB target
    Restore(RestoreCommand),
    /// List backup manifests found under configured prefix
    ListBackups,
    /// Validate environment and dependencies
    VerifyConfig(VerifyConfigCommand),
}

#[derive(Debug, Args)]
pub struct VerifyConfigCommand {
    /// Skip checking bucket reachability and only verify local config + binaries
    #[arg(long, default_value_t = false)]
    pub skip_remote: bool,
}

#[derive(Debug, Args)]
pub struct RestoreCommand {
    /// Backup run id, for example: 20260304T010203Z-ab12cd34
    #[arg(long)]
    pub run_id: String,
    /// Target MongoDB URI used by mongorestore
    #[arg(long)]
    pub target_uri: String,
    /// Optionally restore only a single database
    #[arg(long)]
    pub target_db: Option<String>,
    /// Drop collections before restore (destructive)
    #[arg(long, default_value_t = false)]
    pub drop: bool,
}
