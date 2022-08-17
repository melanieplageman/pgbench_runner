-- TODO: convert this to a bulk copy
DROP TABLE IF EXISTS large_test;
CREATE TABLE large_test (a int, b int, c int, d int);
INSERT INTO large_test select i, i%3, i, i%2 FROM generate_series(1,200000000)i;
/* INSERT INTO large_test select i, i%2, i, i%3 FROM generate_series(1,30000000)i; */
/* INSERT INTO large_test select i, i%2, i, i%3 FROM generate_series(1,300000000)i; */
INSERT INTO large_test select i, i%2, i, i%3 FROM generate_series(1,300000000)i;
INSERT INTO large_test select i, i%8, i, i%2 FROM generate_series(1,2000000)i;
INSERT INTO large_test select i, i%8, i, i%2 FROM generate_series(1,5000000)i;
INSERT INTO large_test select i, i%8, i, i%2 FROM generate_series(1,50)i;
ANALYZE large_test;
