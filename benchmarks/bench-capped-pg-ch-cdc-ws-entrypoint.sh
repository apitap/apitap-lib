#!/bin/sh
# walshadow container entrypoint, for the PG16 module.
#
# Identical in shape to benchmarks/bench-capped-pg-ch-ws-entrypoint.sh (which
# serves the pg18 module and the bulk arm's PG18 source) with the one difference
# that matters: this campaign's source is PostgreSQL 16, so the image is built
# from postgres:16 with walshadow-pgext-0.1.2-pg16 — the release ships one module
# per major and the module must match the source, because the shadow PostgreSQL
# this daemon supervises is a physical copy of the source.
#
# Modelled on the flag set upstream's own docker/entrypoint.sh passes, because the
# container IS walshadow's deployment unit: the daemon plus the shadow PostgreSQL
# it owns and supervises. Anything this benchmark caps, it caps in one container
# exactly as upstream deploys it.
#
# `init` and `ctl` run as themselves so `docker run ... init` and
# `docker run ... ctl status` work against the same binary without state dirs.
set -eu

case "${1:-}" in
    init | ctl)
        exec walshadow-stream "$@"
        ;;
    # The floor probe: load the binary, touch no state, need no source. It runs
    # the SAME probe script apitap's leg runs, so both floors are measured the
    # same way; /job is bind-mounted read-only for exactly this.
    import)
        exec sh /job/bench-capped-pg-ch-probes.sh import ws
        ;;
esac

DATA="${WALSHADOW_DATA:-/var/lib/walshadow}"
SHADOW_DATA="${WALSHADOW_SHADOW_DATA:-$DATA/shadow-data}"
OUT_DIR="${WALSHADOW_OUT_DIR:-$DATA/out}"
SPILL_DIR="${WALSHADOW_SPILL_DIR:-$DATA/spill}"
SOCKET_DIR="${WALSHADOW_SHADOW_SOCKET_DIR:-/var/run/postgresql}"
CH_CONFIG="${WALSHADOW_CH_CONFIG:-/etc/walshadow/ch-config.toml}"

mkdir -p "$SHADOW_DATA"
# Shadow PG refuses to start on a data dir that is not 0700/0750; a bind mount
# can drop the mode, so reassert it on every boot.
chmod 700 "$SHADOW_DATA"
mkdir -p "$OUT_DIR" "$SPILL_DIR" "$SOCKET_DIR" "${CH_CONFIG%.toml}.d"

if [ -z "${WALSHADOW_PG_URL:-}" ] && [ -z "${WALSHADOW_SOURCE_HOST:-}" ] \
   && [ ! -f "$CH_CONFIG" ]; then
    echo "walshadow: no source configured. Set WALSHADOW_PG_URL or mount a" \
         "config at $CH_CONFIG (write one with 'init')." >&2
    exit 64
fi

# Upstream injects --xact-buffer-max 1073741824 unless the caller already
# passed it, and only overrides the pool sizes when the caller exported them.
# Kept identical here, with env overrides, because the whole point of the capped
# arm is to measure walshadow as upstream ships it.
case " $* " in
    *" --xact-buffer-max "*) ;;
    *) set -- --xact-buffer-max "${WALSHADOW_XACT_BUFFER_MAX:-1073741824}" "$@" ;;
esac
if [ -n "${WALSHADOW_INSERTER_POOL:-}" ]; then
    case " $* " in
        *" --inserter-pool-size "*) ;;
        *) set -- --inserter-pool-size "$WALSHADOW_INSERTER_POOL" "$@" ;;
    esac
fi
if [ -n "${WALSHADOW_DECODER_POOL:-}" ]; then
    case " $* " in
        *" --decoder-pool-size "*) ;;
        *) set -- --decoder-pool-size "$WALSHADOW_DECODER_POOL" "$@" ;;
    esac
fi

set -- \
    --out-dir "$OUT_DIR" \
    --spill-dir "$SPILL_DIR" \
    --shadow-socket-dir "$SOCKET_DIR" \
    --shadow-port "${WALSHADOW_SHADOW_PORT:-5442}" \
    --shadow-user postgres \
    --shadow-dbname postgres \
    --bootstrap-mode direct \
    --bootstrap-shadow-data-dir "$SHADOW_DATA" \
    --walsender-bind "${WALSHADOW_WALSENDER_BIND:-127.0.0.1:5433}" \
    --ch-config "$CH_CONFIG" \
    --metrics-bind "${WALSHADOW_METRICS_BIND:-0.0.0.0:9484}" \
    --control-socket "${WALSHADOW_CONTROL_SOCKET:-/var/run/walshadow/control.sock}" \
    --status-interval "${WALSHADOW_STATUS_INTERVAL:-5}" \
    "$@"

if [ -z "${WALSHADOW_PG_URL:-}" ] && [ -n "${WALSHADOW_SOURCE_HOST:-}" ]; then
    set -- \
        --host "$WALSHADOW_SOURCE_HOST" \
        --port "${WALSHADOW_SOURCE_PORT:-5432}" \
        --user "${WALSHADOW_SOURCE_USER:-postgres}" \
        --dbname "${WALSHADOW_SOURCE_DB:-postgres}" \
        --sslmode "${WALSHADOW_SOURCE_SSLMODE:-disable}" \
        "$@"
fi

echo "WS_ARGV $*" >&2
exec walshadow-stream "$@"