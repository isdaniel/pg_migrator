#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Is a large single-table migration network-bound?
#
# Run this ON THE MIGRATOR HOST, so every hop it measures is a hop the real
# migration actually pays. It samples a fixed ctid page range (a TID Range
# Scan, not a full table scan), so it costs seconds rather than hours.
#
# What it answers: the current pg_dump -> disk -> pg_restore path moves the
# table across the network TWICE, uncompressed both ways (pg_dump's
# compression happens on the migrator, after the bytes have already crossed
# the wire). A direct source -> target COPY moves it once. If the link is
# the bottleneck, that halving is the entire available win.
#
# Safe by default: only reads. WRITE_TEST=1 additionally creates and drops
# an UNLOGGED table named _bench_sink on the TARGET.
#
#   SRC='postgresql://...' TGT='postgresql://...' TABLE=public.orders \
#   WRITE_TEST=1 ./scripts/measure_hops.sh
# ---------------------------------------------------------------------------
set -uo pipefail
: "${SRC:?set SRC to the source connection URI}"
: "${TGT:?set TGT to the target connection URI}"
: "${TABLE:?set TABLE to the big unpartitioned table, e.g. public.orders}"
SAMPLE_GB=${SAMPLE_GB:-3}

mb() { echo "scale=1; $1 / 1048576" | bc; }
# Guard every division: a failed hop leaves bytes or seconds at 0, and bc
# aborts the whole script on divide-by-zero rather than returning an error.
rate() {
  if [ "$(echo "$2 > 0" | bc)" = "1" ]; then
    echo "scale=1; ($1 / 1048576) / $2" | bc
  else
    echo "n/a"
  fi
}

PAGES=$(psql "$SRC" -tAc "SELECT relpages FROM pg_class WHERE oid='$TABLE'::regclass") || exit 1
HEAP=$(psql "$SRC" -tAc "SELECT pg_relation_size('$TABLE')") || exit 1
[ -z "$PAGES" ] && {
  echo "table $TABLE not found on source" >&2
  exit 1
}
SAMPLE_PAGES=$((SAMPLE_GB * 131072))
[ "$SAMPLE_PAGES" -gt "$PAGES" ] && SAMPLE_PAGES=$PAGES
RANGE="ctid >= '(0,0)'::tid AND ctid < '($SAMPLE_PAGES,0)'::tid"

echo "table=$TABLE  heap=$(mb "$HEAP") MB  relpages=$PAGES  sampling $SAMPLE_PAGES pages"
echo

# --- hop 1: source -> migrator (the read leg pg_dump pays) -----------------
s=$(date +%s.%N)
BYTES=$(psql "$SRC" -qAt -c "COPY (SELECT * FROM $TABLE WHERE $RANGE) TO STDOUT" | wc -c)
D1=$(echo "$(date +%s.%N) - $s" | bc)
echo "hop1  source -> migrator : $(rate "$BYTES" "$D1") MB/s   ($(mb "$BYTES") MB in ${D1}s)"

# --- hop 2: migrator <-> target, read-only synthetic proxy -----------------
s=$(date +%s.%N)
B2=$(psql "$TGT" -qAt -c "COPY (SELECT i FROM generate_series(1,20000000) i) TO STDOUT" | wc -c)
D2=$(echo "$(date +%s.%N) - $s" | bc)
echo "hop2  target <-> migrator: $(rate "$B2" "$D2") MB/s   (synthetic, read-only proxy)"

if [ "${WRITE_TEST:-0}" != "1" ]; then
  echo
  echo "(set WRITE_TEST=1 to measure the direct source->target path; it creates"
  echo " and drops an UNLOGGED table named _bench_sink on the target)"
  exit 0
fi

# --- hop 0: source -> target direct, never touching the migrator's disk ----
# Build the sink from the SOURCE's column list: the real table does not
# necessarily exist on the target yet, so `CREATE TABLE ... AS SELECT` there
# would fail.
COLS=$(psql "$SRC" -tAc "
  SELECT string_agg(format('%I %s', attname, format_type(atttypid, atttypmod)), ', '
                    ORDER BY attnum)
  FROM pg_attribute
  WHERE attrelid = '$TABLE'::regclass AND attnum > 0 AND NOT attisdropped")
[ -z "$COLS" ] && {
  echo "could not read column list for $TABLE" >&2
  exit 1
}

psql "$TGT" -q -v ON_ERROR_STOP=1 \
  -c "SET client_min_messages = warning" \
  -c "DROP TABLE IF EXISTS _bench_sink" \
  -c "CREATE UNLOGGED TABLE _bench_sink ($COLS)" || exit 1

s=$(date +%s.%N)
psql "$SRC" -qAt -c "COPY (SELECT * FROM $TABLE WHERE $RANGE) TO STDOUT" |
  PGOPTIONS="-c synchronous_commit=off" psql "$TGT" -qAt -c "COPY _bench_sink FROM STDIN"
D0=$(echo "$(date +%s.%N) - $s" | bc)
SUNK=$(psql "$TGT" -tAc "SELECT count(*) FROM _bench_sink")
psql "$TGT" -q -c "DROP TABLE IF EXISTS _bench_sink" >/dev/null
echo "hop0  source -> target  : $(rate "$BYTES" "$D0") MB/s   (direct, 1 stream, $SUNK rows)"

echo
echo "--- extrapolated to the full table ---"
SCALE=$(echo "scale=4; $HEAP / $BYTES" | bc)
printf 'path A/D (via migrator, 2 hops): %s min\n' "$(echo "scale=1; $SCALE * ($D1 + $D0) / 60" | bc)"
printf 'path C   (direct, 1 stream)    : %s min\n' "$(echo "scale=1; $SCALE * $D0 / 60" | bc)"
echo
echo "If those two are far apart, the second hop is real money and a direct"
echo "COPY path is worth building. If they are close, the link is not the"
echo "bottleneck and streaming (pg_dump -Fp | psql) captures most of the win."
