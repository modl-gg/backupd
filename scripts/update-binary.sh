#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="backupd"
BINARY_DEST="/usr/local/bin/backupd"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

RESTART_SERVICE=true

log() {
    printf '[update-binary] %s\n' "$1"
}

die() {
    printf '[update-binary] ERROR: %s\n' "$1" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: ./scripts/update-binary.sh [options]

Builds backupd in release mode and installs the binary to /usr/local/bin/backupd.
This script does not read, write, or validate runtime config values.

Options:
  --no-restart      Do not restart backupd.service after updating the binary
  --help, -h        Show this help text
EOF
}

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        die "run this script as root (example: sudo ./scripts/update-binary.sh)"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --no-restart)
                RESTART_SERVICE=false
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

require_commands() {
    local missing=()
    local cmd
    for cmd in cargo install; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done

    if $RESTART_SERVICE && ! command -v systemctl >/dev/null 2>&1; then
        missing+=("systemctl")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        die "missing required commands: ${missing[*]}"
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

install_binary() {
    log "installing binary to ${BINARY_DEST}"
    install -m 0755 "${REPO_ROOT}/target/release/backupd" "${BINARY_DEST}"
}

maybe_restart_service() {
    if ! $RESTART_SERVICE; then
        log "skipping service restart (--no-restart)"
        return 0
    fi

    if ! systemctl list-unit-files "${SERVICE_NAME}.service" --no-legend 2>/dev/null | grep -q "^${SERVICE_NAME}\.service"; then
        log "${SERVICE_NAME}.service is not installed; binary updated only"
        return 0
    fi

    if systemctl is-active --quiet "${SERVICE_NAME}.service"; then
        log "restarting ${SERVICE_NAME}.service"
        systemctl restart "${SERVICE_NAME}.service"
    else
        log "${SERVICE_NAME}.service is installed but not running; leaving it stopped"
    fi
}

print_completion() {
    cat <<EOF

Binary update complete.

Installed:
  ${BINARY_DEST}

Service restart attempted: ${RESTART_SERVICE}
EOF
}

main() {
    parse_args "$@"
    require_root
    require_commands
    build_binary
    install_binary
    maybe_restart_service
    print_completion
}

main "$@"
