# walshadow 0.1.2 runtime for the capped PostgreSQL -> ClickHouse **CDC** arm.
#
# Same two release artifacts as the bulk arm's image, with the one difference the
# source's major makes necessary: this campaign's source is PostgreSQL 16, so the
# module is `walshadow-pgext-0.1.2-pg16`, and the image is postgres:16 rather than
# postgres:18. The release publishes one module per major (16/17/18) and the
# module has to match the SOURCE: the shadow PostgreSQL this daemon supervises is
# a physical copy of the source, so "physical bootstrap cannot cross major
# versions" applies to the module as much as to the data.
#
# The base is the Debian-flavoured postgres:16 rather than the alpine one because
# the published module links against glibc (ldd shows libc.so.6), so it cannot be
# loaded into a musl image at all.
#
# Nothing is compiled here: both files come from the release tarballs, sha256-
# verified against the published SHA256SUMS.
#
# Build context must contain: walshadow-stream, walshadow.so, ws-entrypoint.sh
FROM postgres:16

COPY walshadow-stream /usr/local/bin/walshadow-stream
COPY walshadow.so /usr/lib/postgresql/16/lib/walshadow.so
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