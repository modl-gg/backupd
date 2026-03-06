#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="backupd"
SERVICE_USER="backupd"
SERVICE_GROUP="backupd"
SERVICE_HOME="/var/lib/backupd"
BINARY_PATH="/usr/local/bin/backupd"
UNIT_PATH="/etc/systemd/system/backupd.service"
CONFIG_DIR="/etc/backupd"

PURGE_CONFIG=false
PURGE_USER=false
ASSUME_YES=false

log() {
    printf '[uninstall] %s\n' "$1"
}

warn() {
    printf '[uninstall] WARNING: %s\n' "$1" >&2
}

die() {
    printf '[uninstall] ERROR: %s\n' "$1" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: ./scripts/uninstall.sh [options]

Options:
  --purge-config     Remove /etc/backupd configuration files
  --purge-user       Remove backupd system user and home directory
  --yes, -y          Auto-confirm prompts
  --help, -h         Show this help text
EOF
}

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
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

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        die "run this script as root (example: sudo ./scripts/uninstall.sh)"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --purge-config)
                PURGE_CONFIG=true
                shift
                ;;
            --purge-user)
                PURGE_USER=true
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

stop_and_disable_service() {
    if systemctl list-unit-files "${SERVICE_NAME}.service" --no-legend 2>/dev/null | grep -q "^${SERVICE_NAME}\.service"; then
        log "stopping and disabling ${SERVICE_NAME}.service"
        systemctl disable --now "${SERVICE_NAME}.service" || true
    else
        warn "${SERVICE_NAME}.service is not registered in systemd"
    fi
}

remove_systemd_unit() {
    if [[ -f "$UNIT_PATH" ]]; then
        log "removing unit file $UNIT_PATH"
        rm -f "$UNIT_PATH"
        systemctl daemon-reload
    fi
}

remove_binary() {
    if [[ -f "$BINARY_PATH" ]]; then
        log "removing binary $BINARY_PATH"
        rm -f "$BINARY_PATH"
    fi
}

maybe_prompt_purge_choices() {
    if ! $PURGE_CONFIG; then
        if prompt_yes_no "Also remove ${CONFIG_DIR}?" "N"; then
            PURGE_CONFIG=true
        fi
    fi

    if ! $PURGE_USER; then
        if prompt_yes_no "Also remove system user '${SERVICE_USER}' and ${SERVICE_HOME}?" "N"; then
            PURGE_USER=true
        fi
    fi
}

remove_config() {
    if $PURGE_CONFIG && [[ -d "$CONFIG_DIR" ]]; then
        log "removing configuration directory $CONFIG_DIR"
        rm -rf "$CONFIG_DIR"
    fi
}

remove_service_user() {
    if ! $PURGE_USER; then
        return 0
    fi

    if id "$SERVICE_USER" >/dev/null 2>&1; then
        log "removing user ${SERVICE_USER}"
        userdel --remove "$SERVICE_USER" || userdel "$SERVICE_USER" || true
    fi

    if getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        groupdel "$SERVICE_GROUP" || true
    fi
}

print_completion() {
    cat <<EOF

Uninstall complete.

Removed:
  - systemd service registration
  - ${UNIT_PATH} (if present)
  - ${BINARY_PATH} (if present)

Optional removals applied:
  purge config: ${PURGE_CONFIG}
  purge user:   ${PURGE_USER}
EOF
}

main() {
    parse_args "$@"
    require_root

    if ! $ASSUME_YES; then
        maybe_prompt_purge_choices
    fi

    stop_and_disable_service
    remove_systemd_unit
    remove_binary
    remove_config
    remove_service_user
    print_completion
}

main "$@"
