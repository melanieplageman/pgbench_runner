# Generate data for 10MB file like this
# COPY (SELECT repeat(random()::text, 5) FROM generate_series(1, 110000)) TO '/tmp/10MB_copytest_data.copy';

# second 10MB file
#COPY (SELECT repeat('a', 100) FROM generate_series(1, 105000)) TO '/tmp/10MB_nonrandom_copytest_data.copy';
