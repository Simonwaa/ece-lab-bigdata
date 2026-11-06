# Lab dbt – Medallion pipeline: answers

## Bronze layer

1. **Benefit of untouched CSV files?** If we find a bug later, we fix the transformation and rebuild silver from the original data. Nothing is lost.

2. **Better format for hourly orders over years?** Parquet files partitioned by date (`year/month/day`), one file per batch, with compaction of small files (or a table format like Iceberg). In dbt, use incremental models so only the new partitions are processed.

## Build

1. **What is a view? When to prefer it?** A view stores only the query and runs it every time it is read (here it reads the CSV on S3 again). It is good for light transformations and fresh data. A table is better when the transformation is heavy or the data is read often.

2. **Size of a UUID as `VARCHAR`?** 36 bytes (32 hex characters and 4 hyphens), against 16 bytes for the `UUID` type.

## Data tests

1. **Which strategy?** For an age dashboard, set invalid birthdates to `NULL`, add `birthdate_is_valid`, and show how many ages are unknown. For sales per region, birthdate is not used, so keep all orders, use `severity: warn` and monitor.

2. **Test for state/ZIP?** A test that joins users with a reference table (state, minimum ZIP, maximum ZIP) and returns the users with no match. The reference data comes from USPS or Census ZIP ranges, loaded as a seed.

## Exercise 4 – questions

- **Where does the data come from?** USPS, Census or GeoNames (Faker also contains ZIP ranges per state).
- **A dbt seed?** Yes for a small table. A full list of ZIP codes is too big, use a source instead.
- **In the silver layer?** Yes, as a test or a flag. Do not delete rows.
- **If the reference data changes?** Tests can fail even if the source did not change. Version the data, then run `dbt seed` and `dbt build` again.

## Exercise 5 – questions

- **Where is the data stored?** View: nowhere, only the query is saved. Table: in the DuckDB file.
- **When is the transformation executed?** View: at every query. Table: once, during `dbt build`.
- **If the CSV changes?** View: the change is visible immediately. Table: old data until the next build.
- **More storage?** The table.
- **Avoids re-reading the source files?** The table.
