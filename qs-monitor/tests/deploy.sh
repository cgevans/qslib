#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
DEPLOY=$ROOT/qs-monitor/packaging/qs-monitor-deploy
TEMPORARY=$(mktemp -d)
trap 'chmod -R u+w "$TEMPORARY" 2>/dev/null || true; rm -rf "$TEMPORARY"' EXIT HUP INT TERM

DOWNLOADS=$TEMPORARY/downloads
BIN=$TEMPORARY/bin
INSTALL_ROOT=$TEMPORARY/opt/qs-monitor
STATE_ROOT=$TEMPORARY/var/lib/qs-monitor
CONFIG=$TEMPORARY/etc/qs-monitor/config.toml
UNIT=$TEMPORARY/etc/systemd/qs-monitor.service
DEPLOY_COMMAND=$TEMPORARY/sbin/qs-monitor-deploy
SERVICE_STATE=$TEMPORARY/service-state
mkdir -p "$DOWNLOADS" "$BIN" "$(dirname "$CONFIG")"
printf '%s\n' '[[machines]]' 'name = "test"' 'host = "127.0.0.1"' > "$CONFIG"
printf '%s\n' inactive > "$SERVICE_STATE"

cat > "$BIN/curl" <<'EOF'
#!/bin/sh
set -eu
output=
url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) output=$2; shift 2 ;;
        --*) shift ;;
        *) url=$1; shift ;;
    esac
done
cp "$FAKE_DOWNLOADS/$(basename "$url")" "$output"
EOF
chmod +x "$BIN/curl"

cat > "$BIN/systemctl" <<'EOF'
#!/bin/sh
set -eu
case "$1" in
    stop)
        printf '%s\n' inactive > "$SERVICE_STATE"
        ;;
    daemon-reload|enable)
        ;;
    start)
        current=$(basename "$(readlink -f "$QS_MONITOR_INSTALL_ROOT/current")")
        case "$current" in
            "${FAIL_VERSION:-}"-*)
                if [ "${FAIL_MODE:-failed}" = timeout ]; then
                    printf '%s\n' activating > "$SERVICE_STATE"
                else
                    printf '%s\n' failed > "$SERVICE_STATE"
                    exit 1
                fi
                ;;
            *) printf '%s\n' active > "$SERVICE_STATE" ;;
        esac
        ;;
    is-active)
        [ "$(cat "$SERVICE_STATE")" = active ]
        ;;
    is-failed)
        [ "$(cat "$SERVICE_STATE")" = failed ]
        ;;
    --no-pager)
        printf 'fake service state: %s\n' "$(cat "$SERVICE_STATE")"
        ;;
    *) exit 2 ;;
esac
EOF
chmod +x "$BIN/systemctl"

make_bundle() {
    version=$1
    executable=${2:-yes}
    work=$TEMPORARY/bundle-$version
    rm -rf "$work"
    mkdir -p "$work"
    sed "s/@VERSION@/$version/" > "$work/qs-monitor" <<'EOF'
#!/bin/sh
case "${1:-}" in
    --version) printf '%s\n' 'qs-monitor @VERSION@' ;;
    --check-config)
        [ -f "$3" ] || exit 1
        ! grep -q invalid "$3"
        ;;
    *) exit 0 ;;
esac
EOF
    if [ "$executable" = yes ]; then
        chmod 0755 "$work/qs-monitor"
    else
        chmod 0644 "$work/qs-monitor"
    fi
    cp "$DEPLOY" "$work/qs-monitor-deploy"
    chmod 0755 "$work/qs-monitor-deploy"
    cp "$ROOT/qs-monitor/packaging/qs-monitor.service" "$work/qs-monitor.service"
    printf '# bundle-version %s\n' "$version" >> "$work/qs-monitor.service"
    cp "$ROOT/qs-monitor/INSTALL.md" "$work/INSTALL.md"
    tag=v$version
    asset=qs-monitor-$tag-x86_64-unknown-linux-gnu.tar.gz
    tar -czf "$DOWNLOADS/$asset" -C "$work" \
        qs-monitor qs-monitor.service qs-monitor-deploy INSTALL.md
    (cd "$DOWNLOADS" && sha256sum "$asset" > "$asset.sha256")
}

run_deploy() {
    env \
        PATH="$BIN:$PATH" \
        FAKE_DOWNLOADS="$DOWNLOADS" \
        FAIL_VERSION="${FAIL_VERSION:-}" \
        FAIL_MODE="${FAIL_MODE:-}" \
        SERVICE_STATE="$SERVICE_STATE" \
        QS_MONITOR_TEST_MODE=1 \
        QS_MONITOR_INSTALL_ROOT="$INSTALL_ROOT" \
        QS_MONITOR_STATE_ROOT="$STATE_ROOT" \
        QS_MONITOR_CONFIG="$CONFIG" \
        QS_MONITOR_UNIT_FILE="$UNIT" \
        QS_MONITOR_DEPLOY_COMMAND="$DEPLOY_COMMAND" \
        QS_MONITOR_SYSTEMCTL="$BIN/systemctl" \
        QS_MONITOR_READY_TIMEOUT=1 \
        QS_MONITOR_REPOSITORY_URL=https://release.invalid/qslib \
        "$DEPLOY" "$@"
}

make_bundle 0.15.2
run_deploy v0.15.2
case "$(basename "$(readlink -f "$INSTALL_ROOT/current")")" in
    0.15.2-*) ;;
    *) exit 1 ;;
esac

make_bundle 0.15.3
printf '%064d  %s\n' 0 qs-monitor-v0.15.3-x86_64-unknown-linux-gnu.tar.gz \
    > "$DOWNLOADS/qs-monitor-v0.15.3-x86_64-unknown-linux-gnu.tar.gz.sha256"
if run_deploy v0.15.3; then
    printf '%s\n' 'bad checksum unexpectedly succeeded' >&2
    exit 1
fi

make_bundle 0.15.3
FAIL_VERSION=0.15.3
FAIL_MODE=timeout
export FAIL_VERSION FAIL_MODE
if run_deploy v0.15.3; then
    printf '%s\n' 'readiness failure unexpectedly succeeded' >&2
    exit 1
fi
unset FAIL_VERSION
unset FAIL_MODE
case "$(basename "$(readlink -f "$INSTALL_ROOT/current")")" in
    0.15.2-*) ;;
    *) printf '%s\n' 'automatic rollback did not restore 0.15.2' >&2; exit 1 ;;
esac
grep -q '# bundle-version 0.15.2' "$UNIT" || {
    printf '%s\n' 'automatic rollback did not restore the previous unit' >&2
    exit 1
}

run_deploy v0.15.3
run_deploy rollback 0.15.2
case "$(basename "$(readlink -f "$INSTALL_ROOT/current")")" in
    0.15.2-*) ;;
    *) printf '%s\n' 'explicit rollback did not select 0.15.2' >&2; exit 1 ;;
esac

make_bundle 0.15.4 no
if run_deploy v0.15.4; then
    printf '%s\n' 'non-executable artifact unexpectedly succeeded' >&2
    exit 1
fi

printf '%s\n' invalid >> "$CONFIG"
make_bundle 0.15.5
if run_deploy v0.15.5; then
    printf '%s\n' 'invalid configuration unexpectedly succeeded' >&2
    exit 1
fi
sed -i '/invalid/d' "$CONFIG"

for version in 0.15.4 0.15.5 0.15.6 0.15.7; do
    make_bundle "$version"
    run_deploy "v$version"
done
count=$(find "$INSTALL_ROOT/releases" -mindepth 1 -maxdepth 1 -type d | wc -l)
[ "$count" -eq 4 ] || {
    printf 'release retention kept %s directories, expected 4\n' "$count" >&2
    exit 1
}

printf '%s\n' 'deployment integration tests passed'
