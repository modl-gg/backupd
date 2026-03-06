#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="backupd"
SERVICE_USER="backupd"
SERVICE_GROUP="backupd"
SERVICE_HOME="/var/lib/backupd"
BINARY_DEST="/usr/local/bin/backupd"
UNIT_DEST="/etc/systemd/system/backupd.service"
CONFIG_DIR="/etc/backupd"
CONFIG_DEST="/etc/backupd/backupd.env"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
UNIT_TEMPLATE="${REPO_ROOT}/packaging/systemd/backupd.service"

NON_INTERACTIVE=false
ASSUME_YES=false
CONFIG_INPUT_FILE=""

MONGODB_URI="${MONGODB_URI:-}"
BACKBLAZE_KEY_ID="${BACKBLAZE_KEY_ID:-}"
BACKBLAZE_APPLICATION_KEY="${BACKBLAZE_APPLICATION_KEY:-}"
BACKBLAZE_ENDPOINT="${BACKBLAZE_ENDPOINT:-}"
BACKBLAZE_REGION="${BACKBLAZE_REGION:-}"
BACKBLAZE_BUCKET_NAME="${BACKBLAZE_BUCKET_NAME:-}"
BACKUP_PREFIX="${BACKUP_PREFIX:-}"
BACKUP_INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-}"
BACKUP_RETENTION_COUNT="${BACKUP_RETENTION_COUNT:-}"
BACKUP_ENCRYPTION_PASSPHRASE="${BACKUP_ENCRYPTION_PASSPHRASE:-}"
BACKUP_OPLOG_MODE="${BACKUP_OPLOG_MODE:-}"
BACKUP_MAX_RUNTIME_SECONDS="${BACKUP_MAX_RUNTIME_SECONDS:-}"
BACKUP_RUN_ON_START="${BACKUP_RUN_ON_START:-}"
BACKUP_MULTIPART_PART_SIZE_BYTES="${BACKUP_MULTIPART_PART_SIZE_BYTES:-}"
BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES="${BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES:-}"
BACKUP_LOG_FORMAT="${BACKUP_LOG_FORMAT:-}"
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
DISCORD_ROLE_MENTION="${DISCORD_ROLE_MENTION:-}"
RUST_LOG="${RUST_LOG:-}"

log() {
    printf '[install] %s\n' "$1"
}

warn() {
    printf '[install] WARNING: %s\n' "$1" >&2
}

die() {
    printf '[install] ERROR: %s\n' "$1" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: ./scripts/install.sh [options]

Options:
  --config <path>        Load configuration values from a KEY=VALUE file
  --non-interactive      Fail on missing values instead of prompting
  --yes, -y              Auto-confirm installer prompts (for CI/unattended usage)
  --help, -h             Show this help text

Notes:
  - This installer supports Ubuntu 24.04+ with systemd.
  - Run as root (or via sudo).
  - In interactive mode it prompts for required and common optional values.
EOF
}

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        die "run this installer as root (example: sudo ./scripts/install.sh)"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --config)
                [[ $# -ge 2 ]] || die "--config requires a file path"
                CONFIG_INPUT_FILE="$2"
                shift 2
                ;;
            --config=*)
                CONFIG_INPUT_FILE="${1#*=}"
                shift
                ;;
            --non-interactive)
                NON_INTERACTIVE=true
                shift
                ;;
            --yes|-y)
                ASSUME_YES=true
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                die "unknown argument: $1"
                ;;
        esac
    done
}

check_supported_os() {
    [[ -f /etc/os-release ]] || die "cannot detect OS: /etc/os-release is missing"
    # shellcheck disable=SC1091
    source /etc/os-release

    [[ "${ID:-}" == "ubuntu" ]] || die "only Ubuntu is supported by this installer"
    command -v dpkg >/dev/null 2>&1 || die "dpkg is required to compare Ubuntu versions"
    dpkg --compare-versions "${VERSION_ID:-0}" ge "24.04" || die "Ubuntu 24.04+ is required"

    [[ -d /run/systemd/system ]] || die "systemd does not appear to be active on this host"
    command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
}

require_commands() {
    local missing=()
    local cmd
    for cmd in cargo install awk grep sed mktemp runuser useradd groupadd; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        die "missing required commands: ${missing[*]}"
    fi
}

is_supported_key() {
    case "$1" in
        MONGODB_URI|BACKBLAZE_KEY_ID|BACKBLAZE_APPLICATION_KEY|BACKBLAZE_ENDPOINT|BACKBLAZE_REGION|BACKBLAZE_BUCKET_NAME|BACKUP_PREFIX|BACKUP_INTERVAL_SECONDS|BACKUP_RETENTION_COUNT|BACKUP_ENCRYPTION_PASSPHRASE|BACKUP_OPLOG_MODE|BACKUP_MAX_RUNTIME_SECONDS|BACKUP_RUN_ON_START|BACKUP_MULTIPART_PART_SIZE_BYTES|BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES|BACKUP_LOG_FORMAT|DISCORD_WEBHOOK_URL|DISCORD_ROLE_MENTION|RUST_LOG)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

load_config_file() {
    local file_path="$1"
    local raw_line line key value

    [[ -f "$file_path" ]] || die "config file does not exist: $file_path"
    [[ -r "$file_path" ]] || die "config file is not readable: $file_path"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        line="$(trim "$raw_line")"
        [[ -z "$line" ]] && continue
        [[ "${line:0:1}" == "#" ]] && continue

        if [[ "$line" == export[[:space:]]* ]]; then
            line="$(trim "${line#export}")"
        fi

        [[ "$line" == *=* ]] || die "invalid config line: $raw_line"
        key="$(trim "${line%%=*}")"
        value="$(trim "${line#*=}")"

        if [[ "$value" == \"*\" && "$value" == *\" ]]; then
            value="${value:1:${#value}-2}"
            value="${value//\\\\/\\}"
            value="${value//\\\"/\"}"
        elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
            value="${value:1:${#value}-2}"
        fi

        if is_supported_key "$key"; then
            printf -v "$key" '%s' "$value"
        fi
    done < "$file_path"
}

apply_defaults() {
    BACKBLAZE_ENDPOINT="${BACKBLAZE_ENDPOINT:-https://s3.us-east-005.backblazeb2.com}"
    BACKBLAZE_BUCKET_NAME="${BACKBLAZE_BUCKET_NAME:-modl-database-backups}"
    BACKUP_PREFIX="${BACKUP_PREFIX:-mongo-backups}"
    BACKUP_INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-10800}"
    BACKUP_RETENTION_COUNT="${BACKUP_RETENTION_COUNT:-12}"
    BACKUP_OPLOG_MODE="${BACKUP_OPLOG_MODE:-auto}"
    BACKUP_MAX_RUNTIME_SECONDS="${BACKUP_MAX_RUNTIME_SECONDS:-7200}"
    BACKUP_RUN_ON_START="${BACKUP_RUN_ON_START:-false}"
    BACKUP_MULTIPART_PART_SIZE_BYTES="${BACKUP_MULTIPART_PART_SIZE_BYTES:-8388608}"
    BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES="${BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES:-1048576}"
    BACKUP_LOG_FORMAT="${BACKUP_LOG_FORMAT:-json}"
    RUST_LOG="${RUST_LOG:-info}"
}

prompt_value() {
    local prompt_text="$1"
    local current_value="$2"
    local input

    if [[ -n "$current_value" ]]; then
        read -r -p "$prompt_text [$current_value]: " input
        printf '%s' "${input:-$current_value}"
    else
        read -r -p "$prompt_text: " input
        printf '%s' "$input"
    fi
}

prompt_secret_required() {
    local prompt_text="$1"
    local input
    while true; do
        read -r -s -p "$prompt_text: " input
        printf '\n'
        if [[ -n "$input" ]]; then
            printf '%s' "$input"
            return 0
        fi
        warn "value is required"
    done
}

prompt_yes_no() {
    local prompt_text="$1"
    local default_answer="$2"
    local suffix reply normalized

    if [[ "$default_answer" == "Y" ]]; then
        suffix="[Y/n]"
    else
        suffix="[y/N]"
    fi

    if $ASSUME_YES; then
        return 0
    fi

    while true; do
        read -r -p "$prompt_text $suffix " reply
        reply="$(trim "$reply")"
        if [[ -z "$reply" ]]; then
            reply="$default_answer"
        fi
        normalized="${reply,,}"
        case "$normalized" in
            y|yes)
                return 0
                ;;
            n|no)
                return 1
                ;;
            *)
                warn "please answer yes or no"
                ;;
        esac
    done
}

collect_configuration() {
    if $NON_INTERACTIVE; then
        return 0
    fi

    log "starting interactive setup wizard"

    MONGODB_URI="$(prompt_value "MongoDB URI (required)" "$MONGODB_URI")"
    BACKBLAZE_KEY_ID="$(prompt_value "Backblaze key id (required)" "$BACKBLAZE_KEY_ID")"
    BACKBLAZE_APPLICATION_KEY="$(prompt_value "Backblaze application key (required)" "$BACKBLAZE_APPLICATION_KEY")"
    if [[ -z "$BACKUP_ENCRYPTION_PASSPHRASE" ]]; then
        BACKUP_ENCRYPTION_PASSPHRASE="$(prompt_secret_required "Backup encryption passphrase (required, 16+ chars)")"
    fi

    BACKBLAZE_ENDPOINT="$(prompt_value "Backblaze endpoint" "$BACKBLAZE_ENDPOINT")"
    BACKBLAZE_REGION="$(prompt_value "Backblaze region (optional, leave empty to auto-derive)" "$BACKBLAZE_REGION")"
    BACKBLAZE_BUCKET_NAME="$(prompt_value "Backblaze bucket name" "$BACKBLAZE_BUCKET_NAME")"
    BACKUP_PREFIX="$(prompt_value "Backup object prefix" "$BACKUP_PREFIX")"
    BACKUP_INTERVAL_SECONDS="$(prompt_value "Backup interval seconds" "$BACKUP_INTERVAL_SECONDS")"
    BACKUP_RETENTION_COUNT="$(prompt_value "Backup retention count" "$BACKUP_RETENTION_COUNT")"
    BACKUP_MAX_RUNTIME_SECONDS="$(prompt_value "Backup max runtime seconds" "$BACKUP_MAX_RUNTIME_SECONDS")"
    BACKUP_OPLOG_MODE="$(prompt_value "Backup oplog mode (auto|off|required)" "$BACKUP_OPLOG_MODE")"
    BACKUP_RUN_ON_START="$(prompt_value "Run backup on startup (true|false)" "$BACKUP_RUN_ON_START")"
    BACKUP_LOG_FORMAT="$(prompt_value "Log format (json|pretty)" "$BACKUP_LOG_FORMAT")"
    DISCORD_WEBHOOK_URL="$(prompt_value "Discord webhook URL (optional)" "$DISCORD_WEBHOOK_URL")"
    DISCORD_ROLE_MENTION="$(prompt_value "Discord role mention (optional)" "$DISCORD_ROLE_MENTION")"
}

is_positive_integer() {
    [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -gt 0 ]]
}

normalize_bool() {
    local value="${1,,}"
    case "$value" in
        1|true|yes|on)
            printf 'true'
            ;;
        0|false|no|off)
            printf 'false'
            ;;
        *)
            return 1
            ;;
    esac
}

validate_configuration() {
    [[ -n "$MONGODB_URI" ]] || die "MONGODB_URI is required"
    [[ -n "$BACKBLAZE_KEY_ID" ]] || die "BACKBLAZE_KEY_ID is required"
    [[ -n "$BACKBLAZE_APPLICATION_KEY" ]] || die "BACKBLAZE_APPLICATION_KEY is required"
    [[ -n "$BACKUP_ENCRYPTION_PASSPHRASE" ]] || die "BACKUP_ENCRYPTION_PASSPHRASE is required"
    [[ "${#BACKUP_ENCRYPTION_PASSPHRASE}" -ge 16 ]] || die "BACKUP_ENCRYPTION_PASSPHRASE must be at least 16 characters"

    is_positive_integer "$BACKUP_INTERVAL_SECONDS" || die "BACKUP_INTERVAL_SECONDS must be a positive integer"
    is_positive_integer "$BACKUP_RETENTION_COUNT" || die "BACKUP_RETENTION_COUNT must be a positive integer"
    is_positive_integer "$BACKUP_MAX_RUNTIME_SECONDS" || die "BACKUP_MAX_RUNTIME_SECONDS must be a positive integer"
    is_positive_integer "$BACKUP_MULTIPART_PART_SIZE_BYTES" || die "BACKUP_MULTIPART_PART_SIZE_BYTES must be a positive integer"
    is_positive_integer "$BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES" || die "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES must be a positive integer"
    [[ "$BACKUP_MULTIPART_PART_SIZE_BYTES" -ge 5242880 ]] || die "BACKUP_MULTIPART_PART_SIZE_BYTES must be at least 5 MiB"
    [[ "$BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES" -le "$BACKUP_MULTIPART_PART_SIZE_BYTES" ]] || die "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES must be <= BACKUP_MULTIPART_PART_SIZE_BYTES"

    case "${BACKUP_OPLOG_MODE,,}" in
        auto|off|required) BACKUP_OPLOG_MODE="${BACKUP_OPLOG_MODE,,}" ;;
        *) die "BACKUP_OPLOG_MODE must be one of: auto, off, required" ;;
    esac

    case "${BACKUP_LOG_FORMAT,,}" in
        json|pretty) BACKUP_LOG_FORMAT="${BACKUP_LOG_FORMAT,,}" ;;
        *) die "BACKUP_LOG_FORMAT must be one of: json, pretty" ;;
    esac

    BACKUP_RUN_ON_START="$(normalize_bool "$BACKUP_RUN_ON_START")" || die "BACKUP_RUN_ON_START must be a boolean"
}

setup_mongodb_repo() {
    local codename keyring repo_file

    # shellcheck disable=SC1091
    source /etc/os-release
    codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
    [[ -n "$codename" ]] || die "unable to determine Ubuntu codename for MongoDB apt repository"

    keyring="/usr/share/keyrings/mongodb-server-8.0.gpg"
    repo_file="/etc/apt/sources.list.d/mongodb-org-8.0.list"

    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl gnupg

    if [[ ! -f "$keyring" ]]; then
        curl -fsSL "https://pgp.mongodb.com/server-8.0.asc" | gpg --dearmor -o "$keyring"
    fi

    cat > "$repo_file" <<EOF
deb [ signed-by=$keyring ] https://repo.mongodb.org/apt/ubuntu $codename/mongodb-org/8.0 multiverse
EOF
}

install_mongo_tools() {
    log "installing mongodb-database-tools"
    apt-get update

    if apt-cache show mongodb-database-tools >/dev/null 2>&1; then
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends mongodb-database-tools
    else
        log "mongodb-database-tools not available in current apt sources; adding MongoDB repo"
        setup_mongodb_repo
        apt-get update
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends mongodb-database-tools
    fi

    command -v mongodump >/dev/null 2>&1 || die "mongodump is still unavailable after installation"
    command -v mongorestore >/dev/null 2>&1 || die "mongorestore is still unavailable after installation"
}

ensure_mongo_tools() {
    if command -v mongodump >/dev/null 2>&1 && command -v mongorestore >/dev/null 2>&1; then
        return 0
    fi

    warn "mongodump and/or mongorestore not found on PATH"

    if $NON_INTERACTIVE; then
        if $ASSUME_YES; then
            install_mongo_tools
            return 0
        fi
        die "database tools are missing; install them or rerun with --yes to auto-install"
    fi

    if prompt_yes_no "Install mongodb-database-tools now?" "Y"; then
        install_mongo_tools
    else
        die "cannot continue without mongodump and mongorestore"
    fi
}

build_binary() {
    [[ -f "${REPO_ROOT}/Cargo.toml" ]] || die "Cargo.toml not found at repository root: ${REPO_ROOT}"

    log "building backupd binary from source"
    (
        cd "$REPO_ROOT"
        cargo build --release --locked
    )

    [[ -f "${REPO_ROOT}/target/release/backupd" ]] || die "build finished but target/release/backupd was not found"
}

create_service_account() {
    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        groupadd --system "$SERVICE_GROUP"
    fi

    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        useradd \
            --system \
            --gid "$SERVICE_GROUP" \
            --home-dir "$SERVICE_HOME" \
            --create-home \
            --shell /usr/sbin/nologin \
            "$SERVICE_USER"
    fi

    install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$SERVICE_HOME"
    install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
}

escape_env_value() {
    local value="$1"
    [[ "$value" != *$'\n'* ]] || die "configuration values must not contain newlines"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf '"%s"' "$value"
}

write_env_line() {
    local file_path="$1"
    local key="$2"
    local value="$3"
    printf '%s=%s\n' "$key" "$(escape_env_value "$value")" >> "$file_path"
}

write_config_file() {
    local tmp_file
    tmp_file="$(mktemp)"

    {
        printf '# Generated by scripts/install.sh on %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        printf '# backupd runtime configuration\n'
    } > "$tmp_file"

    write_env_line "$tmp_file" "MONGODB_URI" "$MONGODB_URI"
    write_env_line "$tmp_file" "BACKBLAZE_KEY_ID" "$BACKBLAZE_KEY_ID"
    write_env_line "$tmp_file" "BACKBLAZE_APPLICATION_KEY" "$BACKBLAZE_APPLICATION_KEY"
    write_env_line "$tmp_file" "BACKBLAZE_ENDPOINT" "$BACKBLAZE_ENDPOINT"
    write_env_line "$tmp_file" "BACKBLAZE_REGION" "$BACKBLAZE_REGION"
    write_env_line "$tmp_file" "BACKBLAZE_BUCKET_NAME" "$BACKBLAZE_BUCKET_NAME"
    write_env_line "$tmp_file" "BACKUP_PREFIX" "$BACKUP_PREFIX"
    write_env_line "$tmp_file" "BACKUP_INTERVAL_SECONDS" "$BACKUP_INTERVAL_SECONDS"
    write_env_line "$tmp_file" "BACKUP_RETENTION_COUNT" "$BACKUP_RETENTION_COUNT"
    write_env_line "$tmp_file" "BACKUP_ENCRYPTION_PASSPHRASE" "$BACKUP_ENCRYPTION_PASSPHRASE"
    write_env_line "$tmp_file" "BACKUP_OPLOG_MODE" "$BACKUP_OPLOG_MODE"
    write_env_line "$tmp_file" "BACKUP_MAX_RUNTIME_SECONDS" "$BACKUP_MAX_RUNTIME_SECONDS"
    write_env_line "$tmp_file" "BACKUP_RUN_ON_START" "$BACKUP_RUN_ON_START"
    write_env_line "$tmp_file" "BACKUP_MULTIPART_PART_SIZE_BYTES" "$BACKUP_MULTIPART_PART_SIZE_BYTES"
    write_env_line "$tmp_file" "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES" "$BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES"
    write_env_line "$tmp_file" "BACKUP_LOG_FORMAT" "$BACKUP_LOG_FORMAT"
    write_env_line "$tmp_file" "DISCORD_WEBHOOK_URL" "$DISCORD_WEBHOOK_URL"
    write_env_line "$tmp_file" "DISCORD_ROLE_MENTION" "$DISCORD_ROLE_MENTION"
    write_env_line "$tmp_file" "RUST_LOG" "$RUST_LOG"

    install -m 0640 -o root -g "$SERVICE_GROUP" "$tmp_file" "$CONFIG_DEST"
    rm -f "$tmp_file"
}

install_binary() {
    install -m 0755 "${REPO_ROOT}/target/release/backupd" "$BINARY_DEST"
}

install_unit() {
    [[ -f "$UNIT_TEMPLATE" ]] || die "systemd unit template not found: $UNIT_TEMPLATE"
    install -m 0644 "$UNIT_TEMPLATE" "$UNIT_DEST"
    systemctl daemon-reload
}

verify_config() {
    log "running backupd verify-config"

    if command -v systemd-run >/dev/null 2>&1; then
        systemd-run \
            --wait \
            --quiet \
            --collect \
            --property="User=${SERVICE_USER}" \
            --property="Group=${SERVICE_GROUP}" \
            --property="EnvironmentFile=${CONFIG_DEST}" \
            "$BINARY_DEST" verify-config
    else
        local -a env_args=(
            "PATH=/usr/local/bin:/usr/bin:/bin"
            "MONGODB_URI=$MONGODB_URI"
            "BACKBLAZE_KEY_ID=$BACKBLAZE_KEY_ID"
            "BACKBLAZE_APPLICATION_KEY=$BACKBLAZE_APPLICATION_KEY"
            "BACKBLAZE_ENDPOINT=$BACKBLAZE_ENDPOINT"
            "BACKBLAZE_REGION=$BACKBLAZE_REGION"
            "BACKBLAZE_BUCKET_NAME=$BACKBLAZE_BUCKET_NAME"
            "BACKUP_PREFIX=$BACKUP_PREFIX"
            "BACKUP_INTERVAL_SECONDS=$BACKUP_INTERVAL_SECONDS"
            "BACKUP_RETENTION_COUNT=$BACKUP_RETENTION_COUNT"
            "BACKUP_ENCRYPTION_PASSPHRASE=$BACKUP_ENCRYPTION_PASSPHRASE"
            "BACKUP_OPLOG_MODE=$BACKUP_OPLOG_MODE"
            "BACKUP_MAX_RUNTIME_SECONDS=$BACKUP_MAX_RUNTIME_SECONDS"
            "BACKUP_RUN_ON_START=$BACKUP_RUN_ON_START"
            "BACKUP_MULTIPART_PART_SIZE_BYTES=$BACKUP_MULTIPART_PART_SIZE_BYTES"
            "BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES=$BACKUP_ENCRYPTION_CHUNK_SIZE_BYTES"
            "BACKUP_LOG_FORMAT=$BACKUP_LOG_FORMAT"
            "DISCORD_WEBHOOK_URL=$DISCORD_WEBHOOK_URL"
            "DISCORD_ROLE_MENTION=$DISCORD_ROLE_MENTION"
            "RUST_LOG=$RUST_LOG"
        )

        runuser -u "$SERVICE_USER" -- env "${env_args[@]}" "$BINARY_DEST" verify-config
    fi
}

enable_and_start_service() {
    systemctl enable --now "${SERVICE_NAME}.service"
    systemctl is-active --quiet "${SERVICE_NAME}.service" || die "service failed to start"
}

print_completion() {
    cat <<EOF

Installation complete.

Service:
  systemctl status ${SERVICE_NAME}.service
  journalctl -u ${SERVICE_NAME}.service -f

Config:
  ${CONFIG_DEST}
EOF
}

main() {
    parse_args "$@"
    require_root
    check_supported_os
    require_commands

    if [[ -n "$CONFIG_INPUT_FILE" ]]; then
        log "loading configuration from ${CONFIG_INPUT_FILE}"
        load_config_file "$CONFIG_INPUT_FILE"
    fi

    apply_defaults
    collect_configuration
    validate_configuration
    ensure_mongo_tools
    build_binary
    create_service_account
    install_binary
    write_config_file
    install_unit
    verify_config
    enable_and_start_service
    print_completion
}

main "$@"
