#! /bin/bash

echo "run with wal_compression zstd"
./s1.sh 'zstd'

echo "run with no wal_compression"
./s1.sh 'off'
