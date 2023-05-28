#! /bin/bash

# echo "running backend_flush_after 128"
# ./s1.sh 128 "off"

# echo "running backend_flush_after 32"
# ./s1.sh 32 "off"

# echo "running backend_flush_after 0"
# ./s1.sh 0 "off"

echo "running backend_flush_after 128, wal_compression zstd"
./s1.sh 128 "zstd"

echo "running backend_flush_after 128"
./s1.sh 128 "off"
