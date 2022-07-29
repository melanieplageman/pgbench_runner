#! /bin/bash

set -e
set -x
# set -v

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Echos either its arguments or STDIN (if called without arguments) to STDERR
# and exits. If STDERR is a TTY then the echoed output will be colored.
fail() {
  [ -t 2 ] && printf "\e[31;01m" 1>&2
  [ $# -eq 0 ] && cat 1>&2 || echo "$@" 1>&2
  [ -t 2 ] && printf "\e[0m" 1>&2
  exit 1
}

usage() {
  local script="$(basename "${0:-${BASH_SOURCE[0]}}")"
  local margin="$(printf '%*s' ${#script})"
while IFS= read -r line; do echo "${line/  /}"; done <<EOF
  Usage: $script [-T TIME] [-s SCALE] [--prewarm] [--load-data] [--trim] HOST

  Run \`pgbench\` on the remote HOST

  Options:
    -T TIME, --time TIME        duration of benchmark test in seconds (default: 3)
    -c CLIENT, --client CLIENT  number of concurrent database clients as well
                                as the number of threads (default: 1)
    -s SCALE, --scale SCALE     report this scale factor in output (default: 2)
    --prewarm                   preload the \`pgbench\` tables into the database

    -h, --help                  show this help message and exit

    --load-data                 if provided, recreate the pgbench database and fill it
                                data at the specified scale
    --trim                      if provided, fstrim will be run on the mountpoint
                                associated with the data directory before the run

    --init                      if provided, re-initialize the Postgres cluster

  Arguments:
    HOST                        remote host where \`pgbench\` should be run
EOF
}

declare -A pgbench=([db]=test [time]=3 [client]=1 [scale]=2 [prewarm]=0)

load_data=0
trim=0
init=0
# Process options in a loop breaking when a non-option argument is encountered
while [[ $# -gt 0 && $1 = -* ]]; do case "$1" in
  -T|--time)   shift; pgbench[time]="$1" ;;
  -c|--client) shift; pgbench[client]="$1" ;;
  -s|--scale)  shift; pgbench[scale]="$1" ;;
  --prewarm)   pgbench[prewarm]=1 ;;
  -h|--help)   usage; exit 0 ;;
  --load-data) load_data=1 ;;
  --trim)      trim=1 ;;
  --init)      init=1 ;;
  *)           usage 1>&2; exit 1 ;;
esac; shift; done

# Only arguments (rather than options) should be left here
if [ $# -gt 1 ]; then usage 1>&2; exit 1; fi

remote="$1"

# Check that jq is available locally
if ! command -v jq > /dev/null; then
  fail 'Please install `jq` to continue'
fi

# Be compatible with both BSD and GNU mktemp
tmpdir="$(mktemp -dt master 2> /dev/null || mktemp -dt master.XXXX)"

SSH=(ssh -o ControlPath="$tmpdir/socket")
SCP=(scp -o ControlPath="$tmpdir/socket")

master_exit() {
  "${SSH[@]}" -O exit $remote &> /dev/null
  rm -rf "$tmpdir"
}
trap master_exit EXIT

# Establish SSH control master indefinitely
"${SSH[@]}" -MNfo ControlPersist=0 $remote

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

# Copy over `specs.json` from the remote host. This file should (roughly) look
# like:
# {
#   "postgres": {
#     "revision": "REL_15_STABLE",
#     "repository": "https://github.com/postgres/postgres.git"
#   },
#   "host": {
#     "instance": {
#       "type": "Standard_D16ds_v4",
#       "limits": {
#         "max_data_disks": 32,
#         "uncached_burst_bw_mbps": 800,
#         "uncached_burst_iops": 32000,
#         "uncached_bw_mbps": 384,
#         "uncached_iops": 25600
#       }
#     },
#     "disk": {
#       "size_gb": 1024,
#       "device": "/dev/disk/azure/scsi1/lun1",
#       "limits": {
#         "size": "p30",
#         "size_gib": 1024,
#         "burst_iops": 5000,
#         "iops": 5000,
#         "burst_bw_mbps": 200,
#         "bw_mbps": 200
#       }
#     }
#   }
# }
"${SCP[@]}" -q "$remote:specs.json" "$tmpdir/specs.json"

# Get system memory
mem_total_bytes="$(ssh_remote awk '$1 == "MemTotal:" { print $2 * 1024; }' /proc/meminfo)"
mem_total_kb=$(("$mem_total_bytes" / 1024))
mem_total_mb=$(($mem_total_kb / 1024))
mem_total_gb=$(($mem_total_mb / 1024))

# Get number of Huge Pages
needed_hugepages=$(echo "($mem_total_bytes*0.8/1)/(2*1024*1024)+1" | bc)
enough_hugepages=$(echo "$needed_hugepages*0.8/1" | bc)

huge_pages_total="$(ssh_remote awk '$1 == "HugePages_Total:" { print $2; }' /proc/meminfo)"
huge_pages_free="$(ssh_remote awk '$1 == "HugePages_Free:" { print $2; }' /proc/meminfo)"
postgres_huge_pages="off"
if [ $huge_pages_free -gt $enough_hugepages ]; then
  postgres_huge_pages="on"
fi

# Get number of CPUS
ncpus=$(ssh_remote lscpu -J | jq '.lscpu | .[] | select(.field == "CPU(s):") | .data | tonumber')

# Restart Postgres
# It is important to ensure that linger is set for this user for logind or
# Postgres will be shutdown whenever we log out
ssh_remote systemctl --user restart ab-postgresql.service

data_directory="$(ssh_remote psql -At -d postgres -c 'SHOW data_directory;')"

if [ $init -eq 1 ]; then
  ssh_remote systemctl --user stop ab-postgresql.service
  ssh_remote rm -rf "$data_directory"
  ssh_remote initdb -D "$data_directory"
  ssh_remote systemctl --user start ab-postgresql.service
  # When re-init'ing the cluster, we must create the test database, even if
  # load-data was not specified, otherwise it won't exist to run pgbench
  ssh_remote createdb
fi

# Create pgbench database
if [ "$load_data" -eq 1 ] ; then
  ssh_remote dropdb --if-exists "${pgbench[db]}"
  ssh_remote createdb
fi

# CREATE EXTENSION pg_prewarm if it will be used
# Note that the pg_prewarm binary must already be built and installed
if [ "${pgbench[prewarm]}" -eq 1 ] ; then
  ssh_remote psql -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" > /dev/null
fi

declare -A set_gucs=(
  # All sizes with identifier after *must* be in MB so that we can do the
  # interpolation of GUC names
  [shared_buffers]=$(($mem_total_mb / 2 ))MB
  [max_wal_size]=$((200 > $mem_total_mb * 2 ? 200 : $mem_total_mb * 2))MB
  [min_wal_size]=$((200 > $mem_total_mb * 2 ? 200 : $mem_total_mb * 2))MB
  [maintenance_work_mem]=$(($mem_total_mb / 29 > 5120 ? 5120 : $mem_total_mb / 29))MB
  [wal_buffers]=$(($mem_total_mb / 250 > 5120 ? 5120 : $mem_total_mb / 250))MB
  [max_connections]=1024
  [autovacuum_vacuum_cost_delay]=1ms
  [autovacuum_freeze_max_age]=2000000000
  [huge_pages]="$postgres_huge_pages"
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

# Reset stats
ssh_remote psql -c "SELECT pg_stat_reset_shared('bgwriter')" > /dev/null
ssh_remote psql -c "SELECT pg_stat_reset()" > /dev/null

# Restart Postgres
ssh_remote systemctl --user restart ab-postgresql.service

# Get current settings of all Postgres system values
ssh_remote psql -At > "$tmpdir/all_gucs.json" <<'EOF'
  SELECT jsonb_object_agg(name, row_to_json(pg_settings)) from pg_settings;
EOF


if [ $trim -eq 1 ] ; then
  dir_to_trim=$(dirname "$data_directory")
  ssh_remote sudo fstrim $dir_to_trim
fi

# TODO: also add mode with pgbench transaction logging to this which tars up
# the logfiles and copies them over
# pgbench --log

# TODO: add a large read that is run with pgbench -f

# Previously, I was using the formula: ($mem_total_gb * 18) to calculate the
# pgbench scale. This is often a good number to try out.

# Fill the pgbench database with data
if [ "$load_data" -eq 1 ] ; then
  ssh_remote pgbench -i -s "${pgbench[scale]}" &> "$tmpdir/pgbench_load.raw"
  python3 pgbench_parse_load.py "$tmpdir/pgbench_load.raw" > "$tmpdir/pgbench_load.json"
else
  jq -n '"skipped"' > "$tmpdir/pgbench_load.json"
fi

# Get pg_stat_bgwriter after loading data into pgbench database
ssh_remote psql -At > "$tmpdir/pg_stat_bgwriter_post_load_pre_run.json" <<'EOF'
  SELECT row_to_json(pg_stat_bgwriter) from pg_stat_bgwriter;
EOF

db_size_post_load_pre_run=$(ssh_remote psql -At -c "SELECT pg_database_size('${pgbench[db]}')")

# Pre-warm the database
if [ "${pgbench[prewarm]}" -eq 1 ] ; then
  ssh_remote psql -c "SELECT pg_prewarm(oid::regclass, 'buffer'), relname FROM pg_class WHERE relname LIKE 'pgbench%'" > /dev/null
fi

pgbench[transaction_type]=tpcb-like

for key in "${!pgbench[@]}"; do
  if [[ "$key" == "scale" || "$key" == "client" || "$key" == "prewarm" || "$key" == "time" ]] ; then
    jq -n --arg key "$key" --arg value "${pgbench[$key]}" '{ ($key): $value | tonumber }'
  else
    jq -n --arg key "$key" --arg value "${pgbench[$key]}" '{ ($key): $value }'
  fi
done | jq -s add > "$tmpdir/pgbench_config.json"

# jq -nf /dev/stdin \
#   --arg client "${pgbench[client]}" \
#   --arg prewarm "${pgbench[prewarm]}" \
#   --arg scale "${pgbench[scale]}" \
#   --arg db "${pgbench[db]}" \
#   --arg time "${pgbench[time]}" \
# <<'EOF'
# EOF

# {
#    "filesystems": [
#       {"target":"/var/lib/autobench1", "source":"/dev/sdc", "fstype":"ext4", "options":"rw,relatime,data=writeback"}
#    ]
# }
filesystem_info=$(ssh_remote findmnt -J $(dirname "$data_directory") | jq '.filesystems[0]')
device_name=$(jq -r '.source' <<< "$filesystem_info")
small_device_name=$(basename $device_name)

declare -A block_device_settings=(
  [nr_requests]=$(ssh_remote cat /sys/block/$small_device_name/queue/nr_requests)
  [scheduler]=$(ssh_remote cat /sys/block/$small_device_name/queue/scheduler)
  [rotational]=$(ssh_remote cat /sys/block/$small_device_name/queue/rotational)
  [wbt_lat_usec]=$(ssh_remote cat /sys/block/$small_device_name/queue/wbt_lat_usec)
  [max_sectors_kb]=$(ssh_remote cat /sys/block/$small_device_name/queue/max_sectors_kb)
  [read_ahead_kb]=$(ssh_remote cat /sys/block/$small_device_name/queue/read_ahead_kb)
  [queue_depth]=$(ssh_remote cat /sys/block/$small_device_name/device/queue_depth)
  [nr_hw_queues]=$(ssh_remote ls -1 /sys/block/$small_device_name/mq | wc -l)
)

# Dump `block_device_settings` as a JSON object to `block_device_settings.json`
for key in "${!block_device_settings[@]}"; do
  if [ "$key" != scheduler ]; then
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value | tonumber }'
  else
    jq -n --arg key "$key" --arg value "${block_device_settings[$key]}" '{ ($key): $value }'
  fi
done | jq -s add > "$tmpdir/block_device_settings.json"

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

# min(ncpus * 6, max_connections) was the rule of thumb I was using for the
# number of clients and jobs

# Run pgbench
ssh_remote pgbench --progress-timestamp \
    -M prepared \
    -c "${pgbench[client]}" \
    -j "${pgbench[client]}" \
    -T "${pgbench[time]}" \
    -P1 \
    --builtin=${pgbench[transaction_type]} \
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

postgres_version="$(ssh_remote psql -At -c 'SELECT version();')"

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
  --arg postgres_version "$postgres_version" \
  --arg data_directory "$data_directory" \
  --arg prewarm "${pgbench[prewarm]}" \
  --arg huge_pages_total "$huge_pages_total" \
  --arg huge_pages_free "$huge_pages_free" \
  --slurpfile specs "$tmpdir/specs.json" \
  --slurpfile all_gucs "$tmpdir/all_gucs.json" \
  --slurpfile pgbench_config "$tmpdir/pgbench_config.json" \
  --arg mem_total_bytes "$mem_total_bytes" \
  --argjson filesystem_info "$filesystem_info" \
  --arg trim "$trim" \
  --arg init "$init" \
  --arg load_data "$load_data" \
  --argjson hostinfo "$hostinfo" \
  --slurpfile set_gucs "$tmpdir/set_gucs.json" \
  --slurpfile pg_stat_bgwriter_post_load_pre_run "$tmpdir/pg_stat_bgwriter_post_load_pre_run.json" \
  --slurpfile pg_stat_bgwriter_post_load_post_run "$tmpdir/pg_stat_bgwriter_post_load_post_run.json" \
  --arg db_size_post_load_pre_run "$db_size_post_load_pre_run" \
  --arg db_size_post_load_post_run "$db_size_post_load_post_run" \
  --slurpfile pgbench_load "$tmpdir/pgbench_load.json" \
  --slurpfile pgbench_progress "$tmpdir/pgbench_progress.json" \
  --slurpfile pgbench_summary "$tmpdir/pgbench_summary.json" \
  --slurpfile iostat "$tmpdir/iostat.json" \
  > "$output_filename" \
<<'EOF'
  {
    metadata: {
      datetime: $datetime,
      machine: {
        instance: ($specs[0].host.instance + {
          hostinfo: $hostinfo,
          huge_pages_free: $huge_pages_free | tonumber,
          huge_pages_total: $huge_pages_total | tonumber,
          mem_total_bytes: $mem_total_bytes | tonumber,
          ncpu: $ncpu | tonumber,
        }),
        disk: ($specs[0].host.disk + {
          block_device_settings: $block_device_settings[0],
          filesystem: $filesystem_info,
          trim: $trim | tonumber,
        }),
      },
      postgres: ($specs[0].postgres + {
        version: $postgres_version,
        data_directory: $data_directory,
        init: $init | tonumber,
        load_data: $load_data | tonumber,
        prewarm: $prewarm | tonumber,
        gucs: {
          all_gucs: $all_gucs[0],
          set_gucs: $set_gucs[0],
        },
      }),
      benchmark: {
        name: "pgbench",
        config: $pgbench_config[0]
      },
    },
    data: {
      pgbench: {
        load: $pgbench_load[0],
        progress: $pgbench_progress[0],
        summary: $pgbench_summary[0],
      },
      iostat: $iostat[0],
    },
    stats: {
      post_load_pre_run: {
        db_size: $db_size_post_load_pre_run | tonumber,
        pg_stat_bgwriter: $pg_stat_bgwriter_post_load_pre_run[0],
      },
      post_load_post_run: {
        db_size: $db_size_post_load_post_run | tonumber,
        pg_stat_bgwriter: $pg_stat_bgwriter_post_load_post_run[0]
      },
    },
  }
EOF
