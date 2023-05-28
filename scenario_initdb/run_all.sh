#! /bin/bash

# echo "walsegsize 16 and doing init"
# ./s1.sh 1 16 0

# pkill postgres
# sleep 5

# echo "walsegsize 16 and no init"
# ./s1.sh 0 16 0

# pkill postgres
# sleep 5

echo "walsegsize 1024 and doing init"
./s1.sh 1 1024 0

pkill postgres
sleep 5

# echo "walsegsize 1024 and no init"
# ./s1.sh 0 1024 0

echo "walsegsize 1024, no init, pause after load"
./s1.sh 0 1024 1
