#! /bin/bash

# set -e
set -x
set -v

dmdelay=$1
read_ahead_kb=$2

pkill postgres
sleep 5
if [ "$dmdelay" != 0 ]; then
    sudo dmsetup remove delayed
    sudo umount /mnt/sabrent
    sudo dmsetup remove delayed
    size=$(sudo blockdev --getsize /dev/nvme1n1)
    echo "0 $size delay "/dev/nvme1n1" 0 $dmdelay" | sudo dmsetup create delayed
    sudo mount -o "data=writeback,noatime" /dev/mapper/delayed /mnt/sabrent
    echo $read_ahead_kb | sudo tee  /sys/block/dm-0/queue/read_ahead_kb
    echo $read_ahead_kb | sudo tee  /sys/block/dm-1/queue/read_ahead_kb
    echo $read_ahead_kb | sudo tee  /sys/block/dm-2/queue/read_ahead_kb
else
    sudo dmsetup remove delayed
    sudo umount /mnt/sabrent
    sudo dmsetup remove delayed
    sudo mount -o "data=writeback,noatime" /dev/nvme1n1 /mnt/sabrent
    echo $read_ahead_kb | sudo tee  /sys/block/nvme1n1/queue/read_ahead_kb
fi
