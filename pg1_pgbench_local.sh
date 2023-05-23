#! /bin/bash

# set -e
set -x
set -v

PRIMARY_PORT=5432
PRIMARY_DATADIR_ROOT="/mnt/sabrent"
PRIMARY_DATADIR="$PRIMARY_DATADIR_ROOT/pgdata"
PRIMARY_INSTALLDIR="/home/mplageman/code/pginstall1/bin"
PRIMARY_BUILDDIR="/home/mplageman/code/pgbuild1"
SOURCEDIR="/home/mplageman/code/pgsource"
PRIMARY_LOGFILE="/tmp/logfile"
DB="test"
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL_PRIMARY=("${PRIMARY_INSTALLDIR}/psql" -p "$PRIMARY_PORT" -d "$DB")

mkdir -p /tmp/pgresults
tmpdir="$(mktemp -d -p /tmp/pgresults 2> /dev/null)"

var_scale=$1
var_backend_flush_after=$2
var_shared_buffers=$3

# Time should be in seconds so parsing works
# declare -A pgbench=([db]=${DB} [time]=60 [client]=10 [scale]=${var_scale})
# declare -A pgbench=([db]=${DB} [time]=1000 [client]=100 [scale]=${var_scale})
# declare -A pgbench=([db]=${DB} [transactions]=1000 [time]=300 [client]=16 [scale]=${var_scale})
declare -A pgbench=([db]=${DB} [transactions]=2000 [time]=30 [client]=16 [scale]=${var_scale})

init=1
load_data=1
do_custom_ddl=0
mixed_workload=0
process_name=checkpointer
pgbench_prewarm=0
do_buffercache=0

pgbench[builtin_script]=tpcb-like

copy_from_source_filename=""
copy_from_source_file_info="{}"
pgbench[custom_filename]=""
if [ "$do_custom_ddl" -eq 1 ] ; then
  # pgbench[custom_filename]=/home/mplageman/code/pgbench_runner/small_copy1.sql

  copy_from_source_filename="/tmp/tiny_copytest_data.copy"
  pgbench[custom_filename]=/home/mplageman/code/pgbench_runner/small_copy3.sql
  if [ "$mixed_workload" -eq 0 ] ; then
    pgbench[builtin_script]=""
  fi

  # copy_from_source_filename="/tmp/copytest_data.copy"
  # pgbench[custom_filename]=/home/mplageman/code/pgbench_runner/small_copy2.sql

  copy_from_source_file_info="$(stat -c '{"filename":"%n","size":%s}' "$copy_from_source_filename" | jq .)"
fi

for key in "${!pgbench[@]}"; do
  if [[ "$key" == "scale" || "$key" == "client" || "$key" == "time" || "$key" == "transactions" ]] ; then
    jq -n --arg key "$key" --arg value "${pgbench[$key]}" '{ ($key): $value | tonumber }'
  else
    jq -n --arg key "$key" --arg value "${pgbench[$key]}" '{ ($key): $value }'
  fi
done | jq -s add > "$tmpdir/pgbench_config.json"

resultsdir=$tmpdir/results
mkdir -p "$resultsdir"
output_filename="$resultsdir/$(uuidgen).json"

datetime=$(date -Iseconds)
hostinfo=$(hostnamectl --json=short)
cpufreq_governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)

# Get system memory
mem_total_kb="$(awk '$1 == "MemTotal:" { print $2; }' /proc/meminfo)"
mem_total_bytes="$(echo "$mem_total_kb * 1024" | bc)"

huge_pages_total="$(awk '$1 == "HugePages_Total:" { print $2; }' /proc/meminfo)"
huge_pages_free="$(awk '$1 == "HugePages_Free:" { print $2; }' /proc/meminfo)"
huge_pages_size_kb="$(awk '$1 == "Hugepagesize:" { print $2 ; }' /proc/meminfo)"

shared_buffers_kb=$(echo "$var_shared_buffers * 8" | bc)

enough_hugepages=$(echo "($shared_buffers_kb / $huge_pages_size_kb) + 5" | bc)

postgres_huge_pages="off"
# if [ $huge_pages_free -gt $enough_hugepages ]; then
#   postgres_huge_pages="on"
# fi

# Get NCPUS
ncpus=$(lscpu -J | jq '.lscpu | .[] | select(.field == "CPU(s):") | .data | tonumber')

# Get postgres build options (can change join to use try catch in case input is not a list)
compile_options=$(meson introspect --buildoptions $PRIMARY_BUILDDIR | jq '.[] | select(.name == "c_args") | .value | join("_")')
build_cassert=$(meson introspect --buildoptions $PRIMARY_BUILDDIR | jq '.[] | select(.name == "cassert") | .value')
build_debug=$(meson introspect --buildoptions $PRIMARY_BUILDDIR | jq '.[] | select(.name == "debug") | .value')

# Get build sha
build_sha=$(git --git-dir=$SOURCEDIR/.git --work-tree=$SOURCEDIR rev-parse --short=7 HEAD)

filesystem_info=$(findmnt -J "$PRIMARY_DATADIR_ROOT" | jq '.filesystems[0]')
device_name=$(jq -r '.source' <<< "$filesystem_info")
# Something with jq to get the name when I use /dev/mapper
# lsblk -Jpo NAME,MOUNTPOINT,PKNAME | jq -r '.blockdevices[] | select(.name == "/dev/sda") | .children[] | .children[] | .pkname'
small_device_name=$(lsblk -no pkname $device_name)

declare -A block_device_settings=(
  [nr_requests]=$(cat /sys/block/$small_device_name/queue/nr_requests)
  [scheduler]=$(cat /sys/block/$small_device_name/queue/scheduler)
  [rotational]=$(cat /sys/block/$small_device_name/queue/rotational)
  [wbt_lat_usec]=$(cat /sys/block/$small_device_name/queue/wbt_lat_usec)
  [max_sectors_kb]=$(cat /sys/block/$small_device_name/queue/max_sectors_kb)
  [read_ahead_kb]=$(cat /sys/block/$small_device_name/queue/read_ahead_kb)
  [queue_depth]=$(cat /sys/block/$small_device_name/device/queue_depth)
  [nr_hw_queues]=$(ls -1 /sys/block/$small_device_name/mq | wc -l)
)

# Dump `block_device_settings` as a JSON object to `block_device_settings.json`
for key in "${!block_device_settings[@]}"; do
  if [ "$key" != scheduler ]; then
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value | tonumber }'
  else
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value }'
  fi
done | jq -s add > "$tmpdir/block_device_settings.json"

truncate --size=0 $PRIMARY_LOGFILE

# Either initdb or restart primary
if [ $init -eq 1 ]; then
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
  "${PRIMARY_INSTALLDIR}/createdb" -p "$PRIMARY_PORT"

  # When re-init'ing the cluster, we must create the test database, even if
  # load-data was not specified, otherwise it won't exist to run pgbench
  ${PRIMARY_INSTALLDIR}/createdb --port "${PRIMARY_PORT}" "${pgbench[db]}"
else
  "${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart
fi


"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '2ms';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET wal_compression = 'off';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET backend_flush_after = '1MB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET max_wal_size = '60GB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET min_wal_size = '10GB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET max_connections = 500;"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET max_prepared_transactions = 1000;"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET track_io_timing=on;"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET log_checkpoints = on;"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET wal_buffers = '1GB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET huge_pages = '$postgres_huge_pages';"

# "${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET backend_flush_after = 0;"
# "${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET shared_buffers = '1GB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET shared_buffers = '${var_shared_buffers}';"
# "${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET checkpoint_timeout = '3min';"
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

"${PSQL_PRIMARY[@]}" -At > "$tmpdir/set_gucs.json" <<'EOF'
    SELECT jsonb_object_agg(name, setting) FROM (SELECT name, setting FROM pg_settings where setting != boot_val);
EOF

killall iostat 2> /dev/null || true

if [ "$load_data" -eq 1 ] ; then
  ${PRIMARY_INSTALLDIR}/dropdb \
    --port "${PRIMARY_PORT}" \
    --if-exists "${pgbench[db]}"

  ${PRIMARY_INSTALLDIR}/createdb \
    --port "${PRIMARY_PORT}" \
    "${pgbench[db]}"
fi

# Create pg_prewarm extension
if [ "$pgbench_prewarm" -eq 1 ] ; then
  "${PSQL_PRIMARY[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm"
fi

if [ "$do_custom_ddl" -eq 1 ] ; then
  client="${pgbench[client]}"

  "${PSQL_PRIMARY[@]}" -v client="$client" <<'EOF'
    SELECT 'BEGIN;'
    UNION ALL
    SELECT format('DROP TABLE IF EXISTS %1$s; CREATE TABLE %1$s(data text not null)', 'copytest_'||g.i)
      FROM generate_series(0, :client) g(i)
    UNION ALL
    SELECT 'COMMIT;'; \gexec
EOF
  # "${PSQL_PRIMARY[@]}" -f /home/mplageman/code/pgbench_runner/small_copy1_ddl.sql
fi

# Make sure OS cache doesn't have any of our data handy
echo 3 | sudo tee /proc/sys/vm/drop_caches

# Restart primary
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

"${PSQL_PRIMARY[@]}" -c "SELECT pg_stat_force_next_flush(); SELECT pg_stat_reset_shared('io')"
"${PSQL_PRIMARY[@]}" -c "SELECT pg_stat_force_next_flush(); SELECT pg_stat_reset_shared('wal')"

"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io; " \
    &> "$tmpdir/pg_stat_io_pre_load_pre_run"

if [ "$load_data" -eq 1 ] ; then
  "${PRIMARY_INSTALLDIR}/pgbench" \
    --port=${PRIMARY_PORT} \
    -i -s "${pgbench[scale]}" \
    "${pgbench[db]}" \
    &> "$tmpdir/pgbench_load_summary.raw"

  python3 /home/mplageman/code/pgbench_runner/pgbench_parse_load_summary.py "$tmpdir/pgbench_load_summary.raw" > "$tmpdir/pgbench_load_summary.json"
else
  jq -n '"skipped"' > "$tmpdir/pgbench_load_summary.json"
fi

db_size_post_load_pre_run=$("${PSQL_PRIMARY[@]}" \
    -At -c \
    "SELECT pg_database_size('${pgbench[db]}')" \
)

"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io;" \
    &> "$tmpdir/pg_stat_io_post_load_pre_run"

"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

# Pre-warm the database
if [ "$pgbench_prewarm" -eq 1 ] ; then
  "${PSQL_PRIMARY[@]}" -c "SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname LIKE 'pgbench%'"
fi

# Dirty writeback
./meminfo.sh &
meminfo_pid=$!

# iostat
S_TIME_FORMAT=ISO iostat -t -y -o JSON -x 1 "$device_name" > iostat.json &
iostat_pid=$!

# TODO: add a check that only one pid is returned by pgrep so that this actually works
# pidstat
pidstat -p $(pgrep -f "$process_name") -d -h -H -l 1 > pidstat_"$process_name".raw &
pidstat_pid=$!

# watch pg_stat_wal
"${PSQL_PRIMARY[@]}" -A -f- <<EOF | head -1 > "$tmpdir/pg_stat_wal_progress.raw"
  SELECT NOW() AS ts, * FROM pg_stat_wal LIMIT 0;
EOF

"${PSQL_PRIMARY[@]}" -At >> "$tmpdir/pg_stat_wal_progress.raw" -f- <<EOF &
  SELECT NOW() AS ts, * FROM pg_stat_wal;
  \watch 1
EOF
pg_stat_wal_progress_pid=$!

# watch pg_stat_activity for vacuum delays
"${PSQL_PRIMARY[@]}" -A -f- <<EOF | head -1 > "$tmpdir/pg_stat_activity_vacuumdelay.raw"
  SELECT NOW() AS ts, null as vacuumdelaywait;
EOF

"${PSQL_PRIMARY[@]}" -At >> "$tmpdir/pg_stat_activity_vacuumdelay.raw" -f- <<EOF &
  SELECT NOW() AS ts, EXISTS(SELECT FROM pg_stat_activity WHERE wait_event IS NOT NULL) AS vacuumdelaywait;
  SELECT NOW() AS ts, EXISTS(SELECT FROM pg_stat_activity WHERE wait_event = 'VacuumDelay') AS vacuumdelaywait;
  \watch 1
EOF
pg_stat_activity_vacuumdelay_pid=$!

# watch pg_stat_io checkpointer
"${PSQL_PRIMARY[@]}" -A -f- <<EOF | head -1 > "$tmpdir/pg_stat_io_checkpointer_progress.raw"
  SELECT NOW() AS ts, * FROM pg_stat_io LIMIT 0;
EOF

"${PSQL_PRIMARY[@]}" -At >> "$tmpdir/pg_stat_io_checkpointer_progress.raw" -f- <<EOF &
  SELECT NOW() AS ts, * FROM pg_stat_io WHERE backend_type = 'checkpointer';
  \watch 1
EOF
pg_stat_io_checkpointer_progress_pid=$!

# watch pg_stat_io normal backend
"${PSQL_PRIMARY[@]}" -A -f- <<EOF | head -1 > "$tmpdir/pg_stat_io_backend_normal_perm_progress.raw"
  SELECT NOW() AS ts, * FROM pg_stat_io LIMIT 0;
EOF

"${PSQL_PRIMARY[@]}" -At >> "$tmpdir/pg_stat_io_backend_normal_perm_progress.raw" -f- <<EOF &
  SELECT NOW() AS ts, * FROM pg_stat_io WHERE backend_type = 'client backend' and context = 'normal' and object = 'relation';
  \watch 1
EOF
pg_stat_io_backend_normal_perm_progress_pid=$!

"${PSQL_PRIMARY[@]}" -A -f- <<EOF | head -1 > "$tmpdir/buffercache_progress.raw"
  SELECT NOW() AS ts, * FROM pg_buffercache_summary() LIMIT 1;
EOF
# watch pg_buffercache_summary()
if [ "$do_buffercache" -eq 1 ]; then
"${PSQL_PRIMARY[@]}" -At >> "$tmpdir/buffercache_progress.raw" -f- <<EOF &
  SELECT NOW() AS ts, * FROM pg_buffercache_summary();
  \watch 5
EOF
buffercache_progress_pid=$!
fi


# TODO: add time command
# TODO: parse out scheduler name
if [ "$do_custom_ddl" -eq 1 ] ; then
  # TODO: add weights for each script to config info
  if [ "$mixed_workload" -eq 1 ]; then
    "${PRIMARY_INSTALLDIR}/pgbench" \
        --port=${PRIMARY_PORT} \
        --progress-timestamp \
        -c "${pgbench[client]}" \
        -j "${pgbench[client]}" \
        -t "${pgbench[transactions]}" \
        -P1 \
        --random-seed=0 \
        --file=${pgbench[custom_filename]}@2 \
        --builtin=${pgbench[builtin_script]}@1 \
        "${pgbench[db]}" \
        > "$tmpdir/pgbench_run_summary.raw" \
        2> "$tmpdir/pgbench_run_progress.raw"
  else
    "${PRIMARY_INSTALLDIR}/pgbench" \
        --port=${PRIMARY_PORT} \
        --progress-timestamp \
        -c "${pgbench[client]}" \
        -j "${pgbench[client]}" \
        -t "${pgbench[transactions]}" \
        -P1 \
        --file=${pgbench[custom_filename]} \
        "${pgbench[db]}" \
        > "$tmpdir/pgbench_run_summary.raw" \
        2> "$tmpdir/pgbench_run_progress.raw"
  fi
else
  "${PRIMARY_INSTALLDIR}/pgbench" \
      --port=${PRIMARY_PORT} \
      --progress-timestamp \
      -M prepared \
      -c "${pgbench[client]}" \
      -j "${pgbench[client]}" \
      -T "${pgbench[time]}" \
      -P1 \
      --random-seed=0 \
      --builtin=${pgbench[builtin_script]} \
      "${pgbench[db]}" \
      > "$tmpdir/pgbench_run_summary.raw" \
      2> "$tmpdir/pgbench_run_progress.raw"
fi


kill -INT $iostat_pid $pidstat_pid $pg_stat_wal_progress_pid $pg_stat_io_checkpointer_progress_pid $pg_stat_activity_vacuumdelay_pid
kill -INT $pg_stat_io_backend_normal_perm_progress_pid
kill $meminfo_pid
if [ "$do_buffercache" -eq 1 ]; then
  kill -INT $buffercache_progress_pid
fi

"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io;" \
    &> "$tmpdir/pg_stat_io_post_load_post_run"

db_size_post_load_post_run=$("${PSQL_PRIMARY[@]}" \
    -At -c \
    "SELECT pg_database_size('${pgbench[db]}')" \
)

pg_stat_wal_after=$("${PSQL_PRIMARY[@]}" -d "$DB" -At -c "select row_to_json(pg_stat_wal) from pg_stat_wal")
pg_stat_io_after=$("${PSQL_PRIMARY[@]}" -d "$DB" -At -c "select row_to_json(pg_stat_io) from pg_stat_io" | jq -s .)

postgres_version="$("${PSQL_PRIMARY[@]}" \
    -At -c 'SELECT version();')"

cp "$PRIMARY_LOGFILE" "$tmpdir/logfile_after"

python3 /home/mplageman/code/pgbench_runner/pgbench_parse_run_progress.py "$tmpdir/pgbench_run_progress.raw" > "$tmpdir/pgbench_run_progress.json"
python3 /home/mplageman/code/pgbench_runner/pgbench_parse_run_summary.py "$tmpdir/pgbench_run_summary.raw" > "$tmpdir/pgbench_run_summary.json"
python3 /home/mplageman/code/pgbench_runner/parse_watch_progress.py "$tmpdir/pg_stat_wal_progress.raw" > "$tmpdir/pg_stat_wal_progress.json"
python3 /home/mplageman/code/pgbench_runner/parse_watch_progress.py "$tmpdir/pg_stat_io_checkpointer_progress.raw" > "$tmpdir/pg_stat_io_checkpointer_progress.json"
python3 /home/mplageman/code/pgbench_runner/parse_watch_progress.py "$tmpdir/pg_stat_io_backend_normal_perm_progress.raw" > "$tmpdir/pg_stat_io_backend_normal_perm_progress.json"
python3 /home/mplageman/code/pgbench_runner/parse_watch_progress.py "$tmpdir/pg_stat_activity_vacuumdelay.raw" > "$tmpdir/pg_stat_activity_vacuumdelay.json"
python3 /home/mplageman/code/pgbench_runner/parse_watch_progress.py "$tmpdir/buffercache_progress.raw" > "$tmpdir/buffercache_progress.json"

# Parse iostat output
cp iostat.json "$tmpdir/iostat.raw"
jq '.sysstat.hosts[0].statistics' < "$tmpdir/iostat.raw" > "$tmpdir/iostat.json"

copy_table_size_after=0

if [ "$do_custom_ddl" -eq 1 ] ; then
  copy_table_size_after="$("${PSQL_PRIMARY[@]}" \
      -At -c "SELECT sum(pg_total_relation_size(relname::regclass)) FROM pg_class WHERE relname LIKE 'copytest%';")"
fi

# Parse pidstat output
# pidstat command should produce output like this
# Time        UID       PID   kB_rd/s   kB_wr/s kB_ccwr/s iodelay  Command

while read -r time uid pid kbrds kbwrs kbccwrs iodelay cmd; do
  jq -n \
    --arg time "$time" \
    --arg kbrds "$kbrds" \
    --arg kbwrs "$kbwrs" \
    --arg kbccwrs "$kbccwrs" \
    --arg iodelay "$iodelay" \
    '{
      "ts": $time | tonumber,
      "kB_rd/s": $kbrds | tonumber,
      "kB_wr/s": $kbwrs | tonumber,
      "kB_ccwr/s": $kbccwrs | tonumber,
      iodelay: $iodelay | tonumber
    }'
done < <(
  head -n -1 "pidstat_${process_name}.raw" | tail -n +4
) | jq -s . > "pidstat_${process_name}.json"

jq -nf /dev/stdin \
  --arg datetime "$datetime" \
  --slurpfile set_gucs "$tmpdir/set_gucs.json" \
  --argjson filesystem_info "$filesystem_info" \
  --argjson hostinfo "$hostinfo" \
  --arg ncpu "$ncpus" \
  --arg data_directory "$PRIMARY_DATADIR" \
  --arg postgres_version "$postgres_version" \
  --slurpfile pgbench_config "$tmpdir/pgbench_config.json" \
  --arg init "$init" \
  --arg load_data "$load_data" \
  --arg db_size_post_load_pre_run "$db_size_post_load_pre_run" \
  --arg db_size_post_load_post_run "$db_size_post_load_post_run" \
  --arg mem_total_bytes "$mem_total_bytes" \
  --slurpfile pgbench_load_summary "$tmpdir/pgbench_load_summary.json" \
  --slurpfile block_device_settings "$tmpdir/block_device_settings.json" \
  --slurpfile pgbench_run_progress "$tmpdir/pgbench_run_progress.json" \
  --slurpfile pgbench_run_summary "$tmpdir/pgbench_run_summary.json" \
  --slurpfile meminfo "meminfo.json" \
  --slurpfile iostat "$tmpdir/iostat.json" \
  --slurpfile pg_stat_wal_progress "$tmpdir/pg_stat_wal_progress.json" \
  --slurpfile pg_stat_io_checkpointer_progress "$tmpdir/pg_stat_io_checkpointer_progress.json" \
  --slurpfile pg_stat_io_backend_normal_perm_progress "$tmpdir/pg_stat_io_backend_normal_perm_progress.json" \
  --slurpfile pg_stat_activity_vacuumdelay "$tmpdir/pg_stat_activity_vacuumdelay.json" \
  --slurpfile pg_buffercache_progress "$tmpdir/buffercache_progress.json" \
  --slurpfile pidstat_data pidstat_"${process_name}".json \
  --arg pidstat_procname "$process_name" \
  --arg build_sha "$build_sha" \
  --arg compile_options "$compile_options" \
  --arg build_debug "$build_debug" \
  --arg build_cassert "$build_cassert" \
  --arg copy_table_size_after "$copy_table_size_after" \
  --arg cpufreq_governor "$cpufreq_governor" \
  --argjson pg_stat_io_after "$pg_stat_io_after" \
  --argjson pg_stat_wal_after "$pg_stat_wal_after" \
  --argjson copy_from_source_file_info "$copy_from_source_file_info" \
  --arg huge_pages_size_kb "$huge_pages_size_kb" \
  --arg pgbench_prewarm "$pgbench_prewarm" \
  --arg mixed_workload "$mixed_workload" \
  > "$output_filename" \
<<'EOF'
  {
    metadata: {
      datetime: $datetime,
      machine: {
        instance: {
          hostinfo: $hostinfo,
          mem_total_bytes: $mem_total_bytes | tonumber,
          huge_pages_size_kb: $huge_pages_size_kb | tonumber,
          ncpu: $ncpu | tonumber,
          cpufreq_governor: $cpufreq_governor,
        },
        disk: {
          block_device_settings: $block_device_settings[0],
          filesystem: $filesystem_info,
        },
      },
      postgres: {
        version: $postgres_version,
        build: {
          sha: $build_sha,
          compile_options: $compile_options,
          debug: $build_debug,
          assert: $build_cassert,
        },
        data_directory: $data_directory,
        init: $init | tonumber,
        load_data: $load_data | tonumber,
        pgbench_prewarm: $pgbench_prewarm | tonumber,
        gucs: $set_gucs[0],
      },
      benchmark: {
        name: "pgbench",
        config: $pgbench_config[0],
        copy_from_source_file_info: $copy_from_source_file_info,
        mixed_workload: $mixed_workload,
      },
    },
    data: {
      pgbench: {
        progress: $pgbench_run_progress[0],
      },
      meminfo: $meminfo[0],
      iostat: $iostat[0],
      walstat: $pg_stat_wal_progress[0],
      iocheckpointerstat: $pg_stat_io_checkpointer_progress[0],
      iobackendnormstat: $pg_stat_io_backend_normal_perm_progress[0],
      vacuumdelaywait : $pg_stat_activity_vacuumdelay[0],
      buffercache: $pg_buffercache_progress[0],
      pidstat: [
        {
          name: $pidstat_procname,
          data: $pidstat_data[0],
        }
      ]
    },
    stats: {
      post_load_pre_run: {
        db_size: $db_size_post_load_pre_run | tonumber,
        pgbench_load_summary: $pgbench_load_summary[0],
      },
      post_load_post_run: {
        db_size: $db_size_post_load_post_run | tonumber,
        pgbench_run_summary: $pgbench_run_summary[0],
        copy_table_size_after: $copy_table_size_after | tonumber,
        pg_stat_io: $pg_stat_io_after,
        pg_stat_wal: $pg_stat_wal_after,
      },
    },
  }
EOF

cp "$output_filename" /home/mplageman/code/bencharts/run_data_local
