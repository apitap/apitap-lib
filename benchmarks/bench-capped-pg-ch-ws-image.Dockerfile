# walshadow 0.1.2 runtime for the capped PostgreSQL -> ClickHouse arm.
#
# The project's own release ships a prebuilt binary per architecture
# (walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz) plus a separate PG
# extension module per PostgreSQL major
# (walshadow-pgext-0.1.2-pg18-x86_64-unknown-linux-gnu.tar.gz), so nothing is
# compiled here: the image is postgres:18 (the major this rig's source runs)
# with those two verified artifacts dropped in. Upstream's own Dockerfile builds
# the module against the runtime PG image so PG_MODULE_MAGIC matches; the
# release module is built for PG18, which is what this image runs.
#
# Build context must contain: walshadow-stream, walshadow.so, ws-entrypoint.sh
FROM postgres:18

COPY walshadow-stream /usr/local/bin/walshadow-stream
COPY walshadow.so /usr/lib/postgresql/18/lib/walshadow.so
COPY ws-entrypoint.sh /usr/local/bin/walshadow-entrypoint
RUN chmod 0755 /usr/local/bin/walshadow-stream /usr/local/bin/walshadow-entrypoint

# State: shadow data dir, filtered WAL (--out-dir), xact spill, control socket.
# Losing all of it costs a re-bootstrap, not correctness.
ENV WALSHADOW_DATA=/var/lib/walshadow
RUN install -d -o postgres -g postgres \
        "$WALSHADOW_DATA" "$WALSHADOW_DATA/shadow-data" "$WALSHADOW_DATA/out" \
        "$WALSHADOW_DATA/spill" /var/run/postgresql /var/run/walshadow \
        /etc/walshadow /etc/walshadow/ch-config.d

USER postgres
ENTRYPOINT ["/usr/local/bin/walshadow-entrypoint"]