#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  Vireon CLI — System Install Script (client-only, no server)
#
#  Installs the `vireon` CLI as a systemd-managed tool:
#    /usr/local/bin/vireon                    — CLI binary
#    /etc/vireon/subscribers/                 — subscriber configs (per-instance)
#    /etc/systemd/system/vireon-sub@.service  — subscriber template unit
#
#  Usage:
#    sudo ./scripts/install.sh                  # install
#    sudo ./scripts/install.sh --uninstall      # remove
#
#  After install:
#    vireon ping
#    sudo systemctl enable --now vireon-sub@<name>
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CLI_BINARY_NAME="vireon"
INSTALL_CLI_BIN="/usr/local/bin/${CLI_BINARY_NAME}"
CONFIG_DIR="/etc/vireon"
SUB_SERVICE_SRC="${REPO_ROOT}/packaging/vireon-sub@.service"
SUB_SERVICE_DST="/etc/systemd/system/vireon-sub@.service"
SUBSCRIBERS_DIR="${CONFIG_DIR}/subscribers"
SUB_CONF_SRC="${REPO_ROOT}/packaging/subscriber.example.conf"
USERNAME="vireon"

G='\033[0;32m'; Y='\033[0;33m'; R='\033[0;31m'; N='\033[0m'
ok()   { echo -e "${G}✓${N} $*"; }
warn() { echo -e "${Y}⚠${N} $*"; }
err()  { echo -e "${R}✗${N} $*" >&2; }

UNINSTALL=false
for arg in "$@"; do
    case "$arg" in
        --uninstall|-u) UNINSTALL=true ;;
        --help|-h) sed -n '2,20p' "$0"; exit 0 ;;
        *) err "Unknown flag: $arg"; exit 1 ;;
    esac
done

if [[ "$(id -u)" -ne 0 ]]; then
    err "Run as root (use sudo)."
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════
#  UNINSTALL
# ═══════════════════════════════════════════════════════════════════
if $UNINSTALL; then
    echo "Uninstalling vireon CLI..."

    for unit in $(systemctl list-units --all 'vireon-sub@*' --no-legend 2>/dev/null | awk '{print $1}'); do
        systemctl disable --now "$unit" 2>/dev/null || true
    done

    rm -f "$INSTALL_CLI_BIN" "$SUB_SERVICE_DST"
    systemctl daemon-reload

    if [[ -d "$CONFIG_DIR" ]]; then
        read -rp "Remove config ($CONFIG_DIR)? [y/N] " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            rm -rf "$CONFIG_DIR"
            ok "removed config dir"
        else
            warn "kept config: $CONFIG_DIR"
        fi
    fi

    if id "$USERNAME" &>/dev/null; then
        userdel "$USERNAME" 2>/dev/null && ok "removed user '$USERNAME'" || warn "could not remove user (running processes?)"
    fi

    ok "uninstall complete"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════
#  INSTALL
# ═══════════════════════════════════════════════════════════════════
echo "Installing vireon CLI..."

# 1. Create system user
if id "$USERNAME" &>/dev/null; then
    ok "user '$USERNAME' already exists"
else
    useradd --system --no-create-home --shell /usr/sbin/nologin "$USERNAME"
    ok "created system user '$USERNAME'"
fi

# 2. Locate + install CLI binary
locate_binary() {
    local name="$1"
    for candidate in \
        "${REPO_ROOT}/target/x86_64-unknown-linux-gnu/release/${name}" \
        "${REPO_ROOT}/target/release/${name}"; do
        if [[ -x "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

if CLI_BINARY="$(locate_binary "$CLI_BINARY_NAME")"; then
    install -m 755 "$CLI_BINARY" "$INSTALL_CLI_BIN"
    ok "installed CLI → $INSTALL_CLI_BIN"
else
    err "CLI binary not found. Build it first: cargo build --release -p vireon-cli"
    exit 1
fi

# 3. Install subscriber template + example config
install -m 644 "$SUB_SERVICE_SRC" "$SUB_SERVICE_DST"
systemctl daemon-reload
mkdir -p "$SUBSCRIBERS_DIR"
if [[ ! -f "$SUBSCRIBERS_DIR/example.conf" ]]; then
    install -m 640 "$SUB_CONF_SRC" "$SUBSCRIBERS_DIR/example.conf"
fi
chown -R root:"${USERNAME}" "$SUBSCRIBERS_DIR"
chmod 750 "$SUBSCRIBERS_DIR"
ok "installed subscriber template → $SUB_SERVICE_DST"

echo
ok "install complete"
echo
echo "  CLI:        $INSTALL_CLI_BIN"
echo "  Test:       vireon ping"
echo "  Subscriber: sudo cp $SUBSCRIBERS_DIR/example.conf $SUBSCRIBERS_DIR/<name>.conf"
echo "  Start:      sudo systemctl enable --now vireon-sub@<name>"
echo "  Logs:       journalctl -u vireon-sub@<name> -f"
