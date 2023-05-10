#! /bin/bash

# set -e
set -x
set -v

PRIMARY_PORT=5432
PRIMARY_DATADIR_ROOT="/mnt/slow"
PRIMARY_DATADIR="$PRIMARY_DATADIR_ROOT/pgdata"
PRIMARY_INSTALLDIR="/home/mplageman/code/pginstall1/bin"
PRIMARY_LOGFILE="/tmp/logfile"
DB="postgres"
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL_PRIMARY=("${PRIMARY_INSTALLDIR}/psql" -p "$PRIMARY_PORT" -d "$DB")

mkdir -p /tmp/pgresults
tmpdir="$(mktemp -d -p /tmp/pgresults 2> /dev/null)"

declare -A pgbench=([db]=${DB} [time]=10 [client]=10 [scale]=10)

init=1
load_data=1
do_custom_ddl=0
process_name=checkpointer

pgbench[builtin_script]=tpcb-like

if [ "$do_custom_ddl" -eq 1 ] ; then
  pgbench[custom_filename]=/home/mplageman/code/pgbench_runner/small_copy.sql
fi

for key in "${!pgbench[@]}"; do
  if [[ "$key" == "scale" || "$key" == "client" || "$key" == "time" ]] ; then
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

# Get system memory
mem_total_bytes="$(awk '$1 == "MemTotal:" { print $2 * 1024; }' /proc/meminfo)"
# Get NCPUS
ncpus=$(lscpu -J | jq '.lscpu | .[] | select(.field == "CPU(s):") | .data | tonumber')

filesystem_info=$(findmnt -J "$PRIMARY_DATADIR_ROOT" | jq '.filesystems[0]')
device_name=$(jq -r '.source' <<< "$filesystem_info")
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

  "${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -l "$PRIMARY_LOGFILE" init
  "${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" start
  "${PRIMARY_INSTALLDIR}/createdb" -p "$PRIMARY_PORT"

  # When re-init'ing the cluster, we must create the test database, even if
  # load-data was not specified, otherwise it won't exist to run pgbench
  ${PRIMARY_INSTALLDIR}/createdb --port "${PRIMARY_PORT}" "{pgbench[db]}"
else
  "${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart
fi

# "${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET backend_flush_after = '1MB';"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET backend_flush_after = 0;"
"${PSQL_PRIMARY[@]}" -c "ALTER SYSTEM SET shared_buffers = '2MB';"
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

"${PSQL_PRIMARY[@]}" -At > "$tmpdir/set_gucs.json" <<'EOF'
    SELECT jsonb_object_agg(name, setting) FROM (SELECT name, setting FROM pg_settings where setting != boot_val);
EOF

killall iostat 2> /dev/null || true

if [ "$load_data" -eq 1 ] ; then
  ${PRIMARY_INSTALLDIR}/dropdb \
    --port "${PRIMARY_PORT}" \
    --if-exists "{pgbench[db]}"

  ${PRIMARY_INSTALLDIR}/createdb \
    --port "${PRIMARY_PORT}" \
    "{pgbench[db]}"
fi

if [ "$do_custom_ddl" -eq 1 ] ; then
  "${PSQL_PRIMARY[@]}" -f /home/mplageman/code/pgbench_runner/small_copy_ddl.sql
fi

# Make sure OS cache doesn't have any of our data handy
echo 3 | sudo tee /proc/sys/vm/drop_caches

# Restart primary
"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

"${PSQL_PRIMARY[@]}" -c "SELECT pg_stat_force_next_flush(); SELECT pg_stat_reset_shared('io')"

"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io; " \
    &> "$tmpdir/pg_stat_io_pre_load_pre_run"

if [ "$load_data" -eq 1 ] ; then
  "${PRIMARY_INSTALLDIR}/pgbench" \
    --port=${PRIMARY_PORT} \
    -i -s "${pgbench[scale]}" \
    "${pgbench[db]}" \
    &> "$tmpdir/pgbench_load.raw"

  python3 /home/mplageman/code/pgbench_runner/pgbench_parse_load.py "$tmpdir/pgbench_load.raw" > "$tmpdir/pgbench_load.json"
else
  jq -n '"skipped"' > "$tmpdir/pgbench_load.json"
fi

db_size_post_load_pre_run=$("${PSQL_PRIMARY[@]}" \
    -At -c \
    "SELECT pg_database_size('${pgbench[db]}')" \
)

"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io;" \
    &> "$tmpdir/pg_stat_io_post_load_pre_run"

"${PRIMARY_INSTALLDIR}/pg_ctl" -D "$PRIMARY_DATADIR" -o "-p $PRIMARY_PORT" -l "$PRIMARY_LOGFILE" restart

# Dirty writeback
./dirty.sh &
dirty_pid=$!

# iostat
S_TIME_FORMAT=ISO iostat -t -y -o JSON -x 1 "$device_name" > iostat.json &
iostat_pid=$!

# pidstat
pidstat -p $(pgrep -f "$process_name") -d -h -H -l 1 > pidstat_"$process_name".raw &
pidstat_pid=$!

# TODO: parse out scheduler name
if [ "$do_custom_ddl" -eq 1 ] ; then
  # TODO: add weights for each script to config info
  "${PRIMARY_INSTALLDIR}/pgbench" \
      --port=${PRIMARY_PORT} \
      --progress-timestamp \
      -M prepared \
      -c "${pgbench[client]}" \
      -j "${pgbench[client]}" \
      -T "${pgbench[time]}" \
      -P1 \
      --random-seed=0 \
      --file=${pgbench[custom_filename]}@2 \
      --builtin=${pgbench[builtin_script]}@1 \
      "${pgbench[db]}" \
      > "$tmpdir/pgbench_summary.raw" \
      2> "$tmpdir/pgbench_progress.raw"
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
      > "$tmpdir/pgbench_summary.raw" \
      2> "$tmpdir/pgbench_progress.raw"
fi


"${PSQL_PRIMARY[@]}" \
    -c "SELECT pg_stat_force_next_flush(); SELECT * FROM pg_stat_io;" \
    &> "$tmpdir/pg_stat_io_post_load_post_run"

db_size_post_load_post_run=$("${PSQL_PRIMARY[@]}" \
    -At -c \
    "SELECT pg_database_size('${pgbench[db]}')" \
)

postgres_version="$("${PSQL_PRIMARY[@]}" \
    -At -c 'SELECT version();')"

cp "$PRIMARY_LOGFILE" "$tmpdir/logfile_after"

kill -INT $dirty_pid $iostat_pid $pidstat_pid

python3 /home/mplageman/code/pgbench_runner/pgbench_parse_progress.py "$tmpdir/pgbench_progress.raw" > "$tmpdir/pgbench_progress.json"
python3 /home/mplageman/code/pgbench_runner/pgbench_parse_summary.py "$tmpdir/pgbench_summary.raw" > "$tmpdir/pgbench_summary.json"

# Parse iostat output
cp iostat.json "$tmpdir/iostat.raw"
jq '.sysstat.hosts[0].statistics' < "$tmpdir/iostat.raw" > "$tmpdir/iostat.json"

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
  --slurpfile pgbench_load "$tmpdir/pgbench_load.json" \
  --slurpfile block_device_settings "$tmpdir/block_device_settings.json" \
  --slurpfile pgbench_progress "$tmpdir/pgbench_progress.json" \
  --slurpfile pgbench_summary "$tmpdir/pgbench_summary.json" \
  --slurpfile dirtywriteback "dirtywriteback.json" \
  --slurpfile iostat "$tmpdir/iostat.json" \
  --slurpfile pidstat_data pidstat_"${process_name}".json \
  --arg pidstat_procname "$process_name" \
  > "$output_filename" \
<<'EOF'
  {
    metadata: {
      datetime: $datetime,
      machine: {
        instance: {
          hostinfo: $hostinfo,
          mem_total_bytes: $mem_total_bytes | tonumber,
          ncpu: $ncpu | tonumber,
        },
        disk: {
          block_device_settings: $block_device_settings[0],
          filesystem: $filesystem_info,
        },
      },
      postgres: {
        version: $postgres_version,
        data_directory: $data_directory,
        init: $init | tonumber,
        load_data: $load_data | tonumber,
        gucs: $set_gucs[0],
      },
      benchmark: {
        name: "pgbench",
        config: $pgbench_config[0],
      },
    },
    data: {
      pgbench: {
        load: $pgbench_load[0],
        progress: $pgbench_progress[0],
        summary: $pgbench_summary[0],
      },
      dirtywriteback: $dirtywriteback[0],
      iostat: $iostat[0],
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
      },
      post_load_post_run: {
        db_size: $db_size_post_load_post_run | tonumber,
      },
    },
  }
EOF

cp "$output_filename" /home/mplageman/code/bencharts/run_data_local
