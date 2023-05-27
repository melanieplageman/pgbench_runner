#! /bin/bash

# echo "default autovac cost delay and default vac buffer usage limit"
# ./s1.sh '2ms' '256kB'

echo "0 autovac cost delay and default vac buffer usage limit"
./s1.sh '0' '256kB'

echo "100 autovac cost delay and default vac buffer usage limit"
./s1.sh '100ms' '256kB'


# echo "default autovac cost delay and 1 GB vac buffer usage limit"
# ./s1.sh '2ms' '1GB'

# echo "100 autovac cost delay and 1 GB vac buffer usage limit"
# ./s1.sh '100ms' '1GB'

# echo "0 autovac cost delay and 1 GB vac buffer usage limit"
# ./s1.sh '0' '1GB'
