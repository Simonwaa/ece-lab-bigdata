# Lab DuckDB – SQL analytics: answers

## Query the bronze layer

1. **Why 40 and 3167?** `approx_unique` gives an estimate (HyperLogLog). It is fast and uses little memory, but it is not exact. For an exact value, use `count(DISTINCT ...)`.

2. **Problem with type inference?** DuckDB guesses the types from the first rows only. If they are not typical, the type can be wrong, for example integers first and text later. This causes an error or lost data (a zip code like `01352` becomes `1352`). Fix: a bigger sample (`sample_size = -1`), explicit types, or `all_varchar`.

## Parquet export

1. **Why is `uuid` barely compressed?** UUIDs are random 36-character strings and almost all different, so there is nothing repeated to compress. The size (about 102 KB) is close to 2829 × 36 bytes.

2. **Why is the compressed size sometimes bigger?** When data is already compact or random, compression gives no gain and adds a small overhead (headers). It happens for `date` and `product`, by only a few bytes.

## Hive partitioning

1. **Why not partition by `uuid`?** There are about 2829 different values, so 2829 folders with tiny files (the "small files problem"). Object storage has no real folders, and listing and opening thousands of objects is slow. The metadata costs more than the data.

2. **Best partition column?** The date (`date=2026-11-06`, or by month if the volume is small). Queries often filter by period, new data goes into a new partition, and the number of partitions stays reasonable.

## CSV vs Parquet at scale

1. **How many row groups, how many skipped?** About 8 row groups of ~123,000 rows. The orders are one hour apart, so the file covers 2020 to about 2134, around 14 years per group. Only the last 3 groups can contain dates from 2100, so about 5 are skipped. Check with the `parquet_metadata` query (`stats_max` ≥ 2100).

2. **If the dates were shuffled?** Each row group would contain dates from the whole period, so the min/max statistics could not exclude any group. All groups would be read.

3. **Network or parsing?** In the lab the S3 server is local, so the gap (0.28 s vs 0.14 s) comes mostly from parsing the CSV and reading all columns. On the platform, the network matters more: the CSV transfers about 111 MiB and the Parquet query about 0.8 MiB. Compare with your own measured times.
