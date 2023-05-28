# This is how the 1MB file is made
# COPY (SELECT repeat(random()::text, 5) FROM generate_series(1, 10000)) TO '/tmp/tiny_copytest_data.copy';
