#! /bin/bash

echo "running with gaussian distribution access, 20 GB shared buffers"
./s1.sh 'gaussian' 20

echo "running with random access, 20 GB shared buffers"
./s1.sh 'random' 20
