#! /usr/bin/env python3

import psycopg2
import sys
import time

sleeptime = sys.argv[1]

time.sleep(int(sleeptime))

connection = psycopg2.connect(
    host="/home/mplageman/.autobench/postgresql",
    dbname="test")

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM large_test WHERE c < 0")
    for record in cursor:
        pass
