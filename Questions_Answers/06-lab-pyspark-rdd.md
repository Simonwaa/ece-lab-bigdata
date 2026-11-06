# Lab PySpark RDD: answers

## RDD creation and partitions

1. **What decides the default number of partitions?** `textFile()` has a `minPartitions` parameter, which is `min(defaultParallelism, 2)`, so 2. Spark splits the file by size, so a small file gets 2 partitions, whatever the number of CPU cores.

2. **`repartition()` vs `coalesce()`?** `repartition()` does a full shuffle. `coalesce()` does not shuffle: it merges existing partitions, so it can only reduce their number.

## Lazy evaluation and lineage

1. **Second `count()`: new stage?** Yes, a new job appears. The RDD is not cached, so Spark recomputes everything (read the file and filter) at each action.

2. **Where would `.cache()` change this?** On `orders_data`, before the first action: `orders_data = orders_lines.filter(...).cache()`. The second `count()` would then read from memory.

## The multiline address

1. **Why does `multiLine=True` work but not `textFile()`?** The CSV reader understands quotes, so a line break inside a quoted field stays in the same record. `textFile()` cuts at every line break without any notion of fields.

2. **What must be true for a line-based reader?** Each line must be one complete record, so no line break inside a record (for example JSON Lines or a CSV without line breaks in fields).

## Manual parsing of orders

1. **Extra comma in `product`?** `split(",")` returns 6 values, so unpacking fails with a `ValueError`. Because of lazy evaluation, the error appears at the first action, not at read time, and the job fails.

2. **Where does the DataFrame API check this?** The CSV reader applies the schema when an action runs, with a mode: `PERMISSIVE` (invalid values become null), `DROPMALFORMED` or `FAILFAST`. So the behavior is configurable.

## Key-value RDDs

1. **`reduceByKey` vs `groupByKey`?** `reduceByKey` combines values inside each partition first, so only small partial results are shuffled. `groupByKey` shuffles every (key, value) pair, which means more network and memory.

2. **When is `groupByKey` unavoidable?** When we need all values of a key together and cannot combine them step by step, for example an exact median or the full list of values.

## Manual deduplication

1. **What replaces `partitionBy()`?** The shuffle of `reduceByKey`: all rows with the same `order_id` go to the same partition. The function `most_recent` replaces the `ORDER BY` and `row_number() = 1`.

2. **Which is clearer?** The window function: it says directly what we want. `reduceByKey` is shorter but hides the rule inside a function.

## Joining orders with users

1. **How to avoid the shuffle?** Partition both RDDs with the same partitioner and the same number of partitions before the join (and cache them). Or broadcast the small RDD.

2. **`leftOuterJoin()`?** Orders without a matching user are kept, with `None` instead of the user. With `join()`, they disappear.

3. **Risk of `collect()`?** It brings all the data to the driver, which can run out of memory. Use `take()`, aggregate first, or write to storage.

## Exercises – discussion

- **Ex. 3:** The RDD version needs manual parsing and several steps. The DataFrame version is one line. `explain()` shows an optimized plan, while `toDebugString()` shows Python functions that Spark cannot optimize.
- **Ex. 4:** Add your timings. Without `.cache()` every action reads and parses the file again. With `.cache()`, only the first action is slow, and the Storage tab shows the cached RDD.
