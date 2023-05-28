# This is how the 20MB file is made
# COPY (SELECT repeat(random()::text, 5) FROM generate_series(1, 210000)) TO '/tmp/medium_copytest_data.copy';
