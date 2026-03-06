# backupd

Rust backup daemon that:
- creates MongoDB archive backups with `mongodump --archive --gzip`
- encrypts client-side with AES-256-GCM (Argon2id-derived key)
- uploads encrypted backups to Backblaze B2 (S3 API)
- enforces retention by run count
- restores backups using `mongorestore`

> [!WARNING]
> **This only works for Ubuntu. That's what our infrastructure runs on so we didn't bother designing installation script for other distros, sorry :/**

## Quick install
```bash
sudo ./scripts/install.sh
```

## Update binary only
```bash
sudo ./scripts/update-binary.sh
```

## Uninstall
```bash
sudo ./scripts/uninstall.sh
```

## CLI commands
```bash
backupd run
backupd backup-now
backupd list-backups
backupd verify-config
backupd verify-config --skip-remote
backupd restore --run-id 20260304T000000Z-ab12cd34 --target-uri "mongodb://localhost:27017"
backupd restore --run-id 20260304T000000Z-ab12cd34 --target-uri "mongodb://localhost:27017" --target-db modl --drop
```
