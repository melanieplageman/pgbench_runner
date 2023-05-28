#! /bin/bash

echo "running with gaussian distribution access, 2 GB shared buffers"
./s1.sh 'gaussian' 2

echo "running with random access, 2 GB shared buffers"
./s1.sh 'random' 2
