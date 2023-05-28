#! /bin/bash

####################
echo running setup s1.sh
./s1.sh

echo running disk setup raw device default settings
./disksetup.sh 0 128

echo running test
./s2.sh 0

echo running disk setup raw device larger settings
./disksetup.sh 0 2048

echo running test
./s2.sh 0

echo running disk setup dmdelay 1ms default settings
./disksetup.sh 1 128

echo running test
./s2.sh '1ms'

echo running disk setup dmdelay 1ms larger settings
./disksetup.sh 1 2048

echo running test
./s2.sh '1ms'

echo running disk setup dmdelay 3ms default settings
./disksetup.sh 3 128

echo running test
./s2.sh '3ms'

echo running disk setup dmdelay 3ms larger settings
./disksetup.sh 3 2048

echo running test
./s2.sh '3ms'
