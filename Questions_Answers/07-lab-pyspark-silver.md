# Lab PySpark silver: answers

## SparkSession with S3 access

1. **Service restarted during the lab?** In `local[*]` mode, the driver and executors run in the same pod, so the job is killed and the work is lost. Also, the credentials are read once when the session starts. Spark does not refresh them, so a long job fails with an expired token. You must start a new session.

2. **Why does `SimpleAWSCredentialsProvider` fail?** It uses only the access key and the secret key. Temporary credentials also need the session token in each request. `TemporaryAWSCredentialsProvider` supports it.

## Silver layer

1. **Explain the output.** The code adds 3 duplicate orders, so the count goes up by 3 (adapt to your numbers), and `groupBy` shows 3 `order_id` with count 2. The window function keeps one row per `order_id` (the most recent), so the count goes back to the original and no duplicate remains. The SQL version gives the same result, so the `assert` lines pass. The last query counts users per state and addresses without a city (military addresses).

2. **DataFrame or SQL?** Both give the same Catalyst plan. For a quick exploration, SQL is faster to write and easy to read. For a model shared with colleagues, DataFrame code in Python functions is better: it can be reused and tested.

3. **Where else can the empty string cause a problem?** In `state` and `zip_code`: if an address does not match, they contain `""` instead of `NULL`. A `groupBy("state")` creates a group with an empty key, a "not null" check passes, and a join with a reference table fails silently.

## Write the silver layer as Parquet

1. **How many `part-*` files?** Add your listing. Spark writes one file per partition, plus a `_SUCCESS` file. `users` usually has one partition, so one file. `orders` has a shuffle, but Spark merges small partitions, so you will see only a few files.

2. **What do you lose with `coalesce(1)`?** Parallelism: one task writes everything, so it is slow and uses a lot of memory. One big file is also harder to read in parallel and a failure restarts the whole write.

## Write the silver layer as Iceberg tables

1. **Which files hold the data?** In `silver/orders.parquet/`: only data files and `_SUCCESS`. In the Iceberg folder, `data/` holds the Parquet files and `metadata/` describes the table (schema, snapshots, file lists, current version).

2. **Overwrite during a read?** With plain Parquet, the reader can see old and new files mixed, partial results, or an error because a file disappeared. With Iceberg, a write creates new files and then switches to the new version in one step, so the reader keeps using its snapshot.

3. **Two jobs commit at the same time?** Both can write the same new metadata file, so one overwrites the other, because S3 has no atomic rename. A catalog service (REST, Nessie, Glue) makes the commit atomic: the second job fails and retries. It also gives central access control.

## Compare with the dbt lab

1. **Schema differences?** dbt keeps `UUID` and `TIMESTAMP WITH TIME ZONE`. In Spark we cast UUIDs to `string`. Consumers see text (36 bytes instead of 16), joins are slower, and the format is no longer guaranteed by the type.

2. **Output layout?** One file per model (dbt) is easier to read. A folder of part-files (Spark) is harder to read but scales better, because it allows parallel writes and reads.

3. **Tests?** This lab only used manual `assert` and visual checks. To get the same guarantees, write explicit checks (`count` vs `countDistinct`, `isNull`, `isin`, `left_anti` join) or use a tool like Great Expectations.

4. **What replaces `ref()` and `source()`?** Nothing explicit: only the order of the cells and the Python variables. For a real pipeline, use an orchestrator like Airflow or Dagster.

5. **Why are the times not comparable?** The engines are different. Spark has a fixed start-up cost (JVM, Iceberg package, session). `dbt build` creates views and runs tests, while Spark writes Parquet and Iceberg files. With so little data, the fixed costs dominate.

6. **dbt with DuckDB or Spark?** 10 GB per day: dbt with DuckDB (one machine is enough). 10 TB per day: Spark (too big for one machine). A team that only knows SQL: dbt (SQL, tests, docs, lineage).

## Exercise 4 – discussion

With Iceberg, `append()` creates a new snapshot, and `VERSION AS OF <snapshot_id>` shows the table before the append (time travel). With a Parquet folder, an append just adds files: there is no history, so you cannot read the previous state, and an overwrite deletes the old data.
