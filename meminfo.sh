#! /bin/bash

echo -n > meminfo_int.json
echo -n > meminfo.json

atexit() {
    jq -s . meminfo_int.json > meminfo.json
}
trap atexit EXIT

while true
do
    jq -n \
        --arg timestamp "$(date -Ins)" \
        --arg dirty_kb "$(awk '$1 == "Dirty:" { print $2 }' /proc/meminfo)" \
        --arg writeback_kb "$(awk '$1 == "Writeback:" { print $2 }' /proc/meminfo)" \
        --arg mem_free_kb "$(awk '$1 == "MemFree:" { print $2 }' /proc/meminfo)" \
        --arg mem_available_kb "$(awk '$1 == "MemAvailable:" { print $2 }' /proc/meminfo)" \
        --arg buffers_kb "$(awk '$1 == "Buffers:" { print $2 }' /proc/meminfo)" \
        '{
            ts: $timestamp,
            dirty_kb: $dirty_kb | tonumber,
            writeback_kb: $writeback_kb | tonumber,
            mem_free_kb: $mem_free_kb | tonumber,
            mem_available_kb: $mem_available_kb | tonumber,
            buffers_kb: $buffers_kb | tonumber
        }' \
        >> meminfo_int.json
    sleep 1
done
