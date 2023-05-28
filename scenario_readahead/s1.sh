#! /bin/bash

# set -e
set -x
set -v

PRIMARY_PORT=5432
PRIMARY_DATADIR_ROOT="/mnt/sabrent"
PRIMARY_DATADIR="$PRIMARY_DATADIR_ROOT/pgdata"
PRIMARY_INSTALLDIR="/home/mplageman/code/pginstall1/bin"
PRIMARY_LOGFILE="/tmp/logfile"
DB="test"
PSQL_PRIMARY=("${PRIMARY_INSTALLDIR}/psql" -p "$PRIMARY_PORT" -d "$DB")

shared_buffers_gb='8'

# Assume file in tmpfs already exists, however, this is the source for creating it
# Generate original data -- this file is just under 5 GB
# "${PSQL_PRIMARY[@]}" -c "COPY (SELECT repeat(random()::text, 5) FROM generate_series(1, 52000000)) TO '/tmp/copytest_data.copy';"

mem_total_kb="$(awk '$1 == "MemTotal:" { print $2; }' /proc/meminfo)"
mem_total_bytes="$(echo "$mem_total_kb * 1024" | bc)"

huge_pages_free="$(awk '$1 == "HugePages_Free:" { print $2; }' /proc/meminfo)"
huge_pages_size_kb="$(awk '$1 == "Hugepagesize:" { print $2 ; }' /proc/meminfo)"

shared_buffers_kb=$(echo "$shared_buffers_gb * 1024 * 1024" | bc)

enough_hugepages=$(echo "($shared_buffers_kb / $huge_pages_size_kb) + 5" | bc)

postgres_huge_pages="off"
if [ $huge_pages_free -gt $enough_hugepages ]; then
  postgres_huge_pages="on"
else
  exit 1
fi

truncate --size=0 $PRIMARY_LOGFILE

"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" status

STATUS="$?"

if [ "$STATUS" -eq 0 ]; then
  echo "database running, must stop, then initdb"
  "${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" stop
fi

if [ $STATUS -eq 4 ]; then
  echo "no valid data dir. need initdb anyway"
fi

if [ $STATUS -eq 3 ]; then
  echo "database stopped. doing initdb"
fi

rm -rf $PRIMARY_DATADIR/*

"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -l "$PRIMARY_LOGFILE" init -o "--wal-segsize=16"
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" start

${PRIMARY_INSTALLDIR}/createdb --port "${PRIMARY_PORT}" "$DB"

"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET huge_pages = '$postgres_huge_pages';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET shared_buffers = '${shared_buffers_gb}GB';"
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

"${PSQL_PRIMARY[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_buffercache;"

"${PSQL_PRIMARY[@]}" -c "DROP TABLE IF EXISTS large_select; CREATE TABLE large_select(data TEXT);"
"${PSQL_PRIMARY[@]}" -c "COPY large_select FROM '/tmp/copytest_data.copy';"
"${PSQL_PRIMARY[@]}" -c "VACUUM (ANALYZE) large_select;"

"${PRIMARY_INSTALLDIR}/pgbench" \
  --port=${PRIMARY_PORT} \
  -i -s "1" \
  "$DB"

"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart
