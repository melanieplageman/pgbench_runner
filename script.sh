#! /bin/bash

# set -e
# set -x

# TODO: filename as uuid
rm -f output.json

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# TODO: make this an argument
remote="10.0.0.4"
# TODO: make this an argument
machine_specs="machine_specs.json"
# TODO: make this an argument
pg_version="master"
# TODO make this an argument
data_directory="/var/lib/autobench1/data"

# TODO: can I do anything about configuring huge pages here?

# Echos either its arguments or STDIN (if called without arguments) to STDERR
# and exits. If STDERR is a TTY then the echoed output will be colored.
fail() {
  [ -t 2 ] && printf "\e[31;01m" 1>&2
  [ $# -eq 0 ] && cat 1>&2 || echo "$@" 1>&2
  [ -t 2 ] && printf "\e[0m" 1>&2
  exit 1
}

# TODO: how to capture that disk was trimmed in metadata (or is that setup?)

# Check that jq is available locally
if ! command -v jq > /dev/null; then
  fail 'Please install `jq` to continue'
fi

# Be compatible with both BSD and GNU mktemp
# tmpdir="$(mktemp -dt master 2> /dev/null || mktemp -dt master.XXXX)"
tmpdir="tmp"

SSH=(ssh -o ControlPath="$tmpdir/socket")
SCP=(scp -o ControlPath="$tmpdir/socket")

master_exit() {
  "${SSH[@]}" -O exit $remote &> /dev/null
  # rm -rf "$tmpdir"
}
trap master_exit EXIT

# Establish SSH control master indefinitely
"${SSH[@]}" -MNfo ControlPersist=0 $remote

declare -A pgbench=([db]=test)

ssh_remote() {
  "${SSH[@]}" $remote env \
    PGDATABASE="${pgbench[db]}" \
    PGHOST='~/.autobench/postgresql' \
    PATH='~/.autobench/postgresql/bin:$PATH' \
    "${@@Q}"
}

# Check that iostat is available on the remote
if ! "${SSH[@]}" $remote command -v iostat > /dev/null; then
  fail 'Please install `iostat` (part of the `sysstat` package) on the target machine to continue'
fi

# Check that lscpu is available on the remote
if ! "${SSH[@]}" $remote command -v lscpu > /dev/null; then
  fail 'Please install `lscpu` on the target machine to continue'
fi

# Get system memory
mem_total_bytes="$(ssh_remote awk '$1 == "MemTotal:" { print $2 * 1024; }' /proc/meminfo)"
mem_total_kb=$(("$mem_total_bytes" / 1024))
mem_total_mb=$(($mem_total_kb / 1024))
mem_total_gb=$(($mem_total_mb / 1024))

# Get number of CPUS
ncpus=$(ssh_remote lscpu -J | jq '.lscpu | .[] | select(.field == "CPU(s):") | .data | tonumber')

# Restart Postgres
ssh_remote systemctl --user restart ab-postgresql.service

# Create pgbench database
ssh_remote dropdb --if-exists "${pgbench[db]}"
ssh_remote createdb

# pg_prewarm should be built and installed
ssh_remote psql -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" > /dev/null


declare -A set_gucs=(
  # All of these *must* be in MB so that we can do the interpolation of GUC names
  [shared_buffers]=$(($mem_total_mb / 2 ))MB
  [max_wal_size]=$((200 > $mem_total_mb * 2 ? 200 : $mem_total_mb * 2))MB
  [min_wal_size]=$((200 > $mem_total_mb * 2 ? 200 : $mem_total_mb * 2))MB
  [maintenance_work_mem]=$(($mem_total_mb / 29 > 5120 ? 5120 : $mem_total_mb / 29))MB
  [wal_buffers]=$(($mem_total_mb / 250 > 5120 ? 5120 : $mem_total_mb / 250))MB
  [max_connections]=1024
  [autovacuum_vacuum_cost_delay]=1ms
  [autovacuum_freeze_max_age]=2000000000

  # TODO: make it harder to screw this up (maybe check if they are configured for OS first)
  [huge_pages]=off
  [backend_flush_after]=256kB
  [checkpoint_completion_target]=0.9
  [log_checkpoints]=on
  [wal_compression]=on
)

# Dump `set_gucs` as a JSON object to `set_gucs.json`
for key in "${!set_gucs[@]}"; do
  jq -n --arg key "$key" --arg value "${set_gucs[$key]}" '{ ($key): $value }'
done | jq -s add > "$tmpdir/set_gucs.json"

# Set GUCs
for key in "${!set_gucs[@]}"; do
  ssh_remote psql -v key="$key" -v value="${set_gucs[$key]}" <<'EOF'
    ALTER SYSTEM SET :"key" = :'value';
EOF
done > /dev/null

# Restart Postgres
ssh_remote systemctl --user restart ab-postgresql.service

# Get current settings of all Postgres system values
ssh_remote psql -At > "$tmpdir/all_gucs.json" <<'EOF'
  SELECT jsonb_object_agg(name, row_to_json(pg_settings)) from pg_settings;
EOF

pgbench[scale]=$(($mem_total_gb * 18))
# pgbench[scale]=2

# TODO: also add mode with pgbench transaction logging to this
# pgbench --log

# Fill the pgbench database with data
ssh_remote pgbench -i -s "${pgbench['scale']}" &> "$tmpdir/pgbench_init.raw"
python3 pgbench_parse_init.py "$tmpdir/pgbench_init.raw" > "$tmpdir/pgbench_init.json"

# Get pg_stat_bgwriter after loading data into pgbench database
ssh_remote psql -At > "$tmpdir/pg_stat_bgwriter_post_load_pre_run.json" <<'EOF'
  SELECT row_to_json(pg_stat_bgwriter) from pg_stat_bgwriter;
EOF

db_size_post_load_pre_run=$(ssh_remote psql -At -c "SELECT pg_database_size('${pgbench[db]}')")

# Pre-warm the database
ssh_remote psql -c "SELECT pg_prewarm(oid::regclass, 'buffer'), relname FROM pg_class WHERE relname LIKE 'pgbench%'" > /dev/null

pgbench['runtime']=1800
pgbench['transaction_type']="tpcb-like"
# TODO: not sure about the number of clients
pgbench['num_clients']=$(("$ncpus" * 6 > ${set_gucs['max_connections']} ? ${set_gucs['max_connections']} : "$ncpus" * 6))

for key in "${!pgbench[@]}"; do
  pgbench_str=$pgbench_str$(printf "%s:\"%s\"," "$key" "${pgbench[$key]}")
done
jq -n {$pgbench_str} > "$tmpdir/pgbench_config.json"

# {
#    "filesystems": [
#       {"target":"/var/lib/autobench1", "source":"/dev/sdc", "fstype":"ext4", "options":"rw,relatime,data=writeback"}
#    ]
# }
filesystem_info=$(ssh_remote findmnt -J $(dirname "$data_directory") | jq '.filesystems[0]')
device_name=$(jq -r '.source' <<< "$filesystem_info")
small_device_name=$(basename $device_name)

# TODO: where to get the hardware queues actually in use
declare -A block_device_settings=(
  [nr_requests]=$(ssh_remote cat /sys/block/$small_device_name/queue/nr_requests)
  [scheduler]=$(ssh_remote cat /sys/block/$small_device_name/queue/scheduler)
  [rotational]=$(ssh_remote cat /sys/block/$small_device_name/queue/rotational)
  [wbt_lat_usec]=$(ssh_remote cat /sys/block/$small_device_name/queue/wbt_lat_usec)
  [max_sectors_kb]=$(ssh_remote cat /sys/block/$small_device_name/queue/max_sectors_kb)
  [read_ahead_kb]=$(ssh_remote cat /sys/block/$small_device_name/queue/read_ahead_kb)
  [queue_depth]=$(ssh_remote cat /sys/block/$small_device_name/device/queue_depth)
  [nr_hw_queues]=$(ssh_remote cat /sys/module/hv_storvsc/parameters/storvsc_max_hw_queues)
)

# Dump `block_device_settings` as a JSON object to `block_device_settings.json`
for key in "${!block_device_settings[@]}"; do
  if [ "$key" != scheduler ]; then
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value | tonumber }'
  else
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value }'
  fi
done | jq -s add > "$tmpdir/block_device_settings.json"

cp $tmpdir/block_device_settings.json .


# We want to start `iostat` before `pgbench` and stop it afterwards. We also
# want JSON output from `iostat`. To ensure that the JSON output from `iostat`
# is complete, we have to:
#
#   1. Ensure that no other instance of `iostat` is running that may clobber
#      our output file, and
#   2. Make sure that it's the `iostat` process itself that receives our
#      termination signal. If, for example, `nohup` receives the SIGTERM first,
#      it can close the file descriptor before `iostat` can properly terminate
#      its JSON output.

ssh_remote killall iostat 2> /dev/null || true

# Create a bash script that will execute `iostat` in the background, emit its
# PID to iostat.pid, and wait.
"${SSH[@]}" -T $remote "cat > iostat.sh" <<'EOF'
#! /bin/bash
S_TIME_FORMAT=ISO iostat -t -o JSON -x 1 "$@" > iostat.json 2> iostat.stderr &
iostat_pid=$!
echo $iostat_pid > iostat.pid
wait $iostat_pid
EOF

# Make the `iostat.sh` script executable
ssh_remote chmod 755 iostat.sh

# We have to `nohup` and redirect both stdout and stderr here or SSH will wait
"${SSH[@]}" $remote "nohup ./iostat.sh ${device_name@Q} &> /dev/null &"

ssh_remote systemctl --user restart ab-postgresql.service
# Run pgbench
ssh_remote pgbench --progress-timestamp \
    -M prepared \
    -c "${pgbench['num_clients']}" \
    -j "${pgbench['num_clients']}" \
    -T "${pgbench['runtime']}" \
    -P1 \
    --builtin=${pgbench['transaction_type']} \
    "${pgbench[db]}" \
    > "$tmpdir/pgbench_summary.raw" \
    2> "$tmpdir/pgbench_progress.raw"

python3 pgbench_parse_progress.py "$tmpdir/pgbench_progress.raw" > "$tmpdir/pgbench_progress.json"
python3 pgbench_parse_summary.py "$tmpdir/pgbench_summary.raw" > "$tmpdir/pgbench_summary.json"

# For some reason, `iostat` won't terminate its JSON output unless it's killed
# with SIGINT rather than SIGTERM
"${SSH[@]}" $remote 'kill -INT $(cat iostat.pid)'

"${SCP[@]}" -q "$remote:iostat.json" "$tmpdir/iostat.raw"

jq '.sysstat.hosts[0].statistics' < "$tmpdir/iostat.raw" > "$tmpdir/iostat.json"

db_size_post_load_post_run=$(ssh_remote psql -At -c "SELECT pg_database_size('${pgbench[db]}')")

# Get pg_stat_bgwriter after running pgbench
ssh_remote psql -At > "$tmpdir/pg_stat_bgwriter_post_load_post_run.json" <<'EOF'
  SELECT row_to_json(pg_stat_bgwriter) from pg_stat_bgwriter;
EOF

# Copy over Postgres log file
"${SCP[@]}" -q "$remote:/tmp/pg_log" "$tmpdir/pg_log"

datetime=$(date -Iseconds)
hostinfo=$(ssh_remote hostnamectl --json=short)

# Make a results directory if it doesn't already exist
resultsdir=results
mkdir -p "$resultsdir"
output_filename="$resultsdir/$(uuidgen).json"

# Put all metadata and data into a file
jq -nf /dev/stdin \
  --arg ncpu "$ncpus" \
  --arg datetime "$datetime" \
  --slurpfile block_device_settings "$tmpdir/block_device_settings.json" \
  --arg postgres_version "$pg_version" \
  --slurpfile machine_specs "$machine_specs" \
  --slurpfile all_gucs "$tmpdir/all_gucs.json" \
  --slurpfile pgbench_config "$tmpdir/pgbench_config.json" \
  --arg mem_total_bytes "$mem_total_bytes" \
  --argjson filesystem_info "$filesystem_info" \
  --argjson hostinfo "$hostinfo" \
  --slurpfile set_gucs "$tmpdir/set_gucs.json" \
  --slurpfile pg_stat_bgwriter_post_load_pre_run "$tmpdir/pg_stat_bgwriter_post_load_pre_run.json" \
  --slurpfile pg_stat_bgwriter_post_load_post_run "$tmpdir/pg_stat_bgwriter_post_load_post_run.json" \
  --arg db_size_post_load_pre_run "$db_size_post_load_pre_run" \
  --arg db_size_post_load_post_run "$db_size_post_load_post_run" \
  --slurpfile pgbench_init "$tmpdir/pgbench_init.json" \
  --slurpfile pgbench_progress "$tmpdir/pgbench_progress.json" \
  --slurpfile pgbench_summary "$tmpdir/pgbench_summary.json" \
  --slurpfile iostat "$tmpdir/iostat.json" \
  > "$output_filename" \
<<'EOF'
  {
    metadata: {
      datetime: $datetime,
      machine: ($machine_specs[0].vm_instance_info.specs + {
        ncpu: $ncpu | tonumber,
        mem_total_bytes: $mem_total_bytes | tonumber,
        filesystem: $filesystem_info,
        disk: {block_device_settings: $block_device_settings[0]},
        hostinfo: $hostinfo
      }),
      postgres: {
        version: $postgres_version,
        gucs: {
          all_gucs: $all_gucs[0],
          set_gucs: $set_gucs[0],
        },
      },
      benchmark: {
        name: "pgbench",
        config: $pgbench_config[0]
      },
    },
    data: {
      pgbench: {
        init: $pgbench_init[0],
        progress: $pgbench_progress[0],
        summary: $pgbench_summary[0],
      },
      iostat: $iostat[0],
    },
    stats: {
      post_load_pre_run: {
        db_size: $db_size_post_load_pre_run,
        pg_stat_bgwriter: $pg_stat_bgwriter_post_load_pre_run[0],
      },
      post_load_post_run: {
        db_size: $db_size_post_load_post_run,
        pg_stat_bgwriter: $pg_stat_bgwriter_post_load_post_run[0]
      },
    },
  }
EOF
