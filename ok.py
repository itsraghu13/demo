from functools import reduce

def smart_jdbc_read(
    spark,
    jdbc_url,
    table_name,
    partition_column,
    lower_bound,
    upper_bound,
    distinct_count,
    total_rows,
    cluster_cores=160,
    avg_row_size_kb=1.6,
    max_jdbc_connections=32,
    force_predicates=False,
    user=None,
    password=None,
    extra_options=None,
    custom_schema=None
):
    """
    Smart JDBC reader that picks optimal partitioning strategy based on input and cluster config.
    """

    def plan():
        if partition_column is None or lower_bound is None or upper_bound is None:
            return {"strategy": "custom_predicates", "reason": "Missing partitioning metadata."}

        if distinct_count < 1000 or distinct_count >= total_rows * 0.9 or force_predicates:
            return {"strategy": "custom_predicates", "reason": "Partition column not suitable."}

        max_ideal_parts = min(max_jdbc_connections, cluster_cores, distinct_count)
        stride = (upper_bound - lower_bound) // max_ideal_parts

        if stride < 1:
            return {"strategy": "custom_predicates", "reason": "Stride too small."}

        return {
            "strategy": "range",
            "numPartitions": max_ideal_parts,
            "stride": stride
        }

    plan_result = plan()

    if plan_result['strategy'] == 'range':
        print(f"\u2705 Using range partitioning with {plan_result['numPartitions']} partitions")
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", plan_result['numPartitions']) \
            .option("fetchsize", 10000)

        if extra_options:
            for k, v in extra_options.items():
                df = df.option(k, v)

        return df.load()

    else:
        print("\u26a0\ufe0f Using custom predicates due to:", plan_result.get("reason", "fallback"))
        predicates = [f"{partition_column} = {v}" for v in range(lower_bound, upper_bound + 1)]
        batch_size = 100
        predicate_batches = [predicates[i:i + batch_size] for i in range(0, len(predicates), batch_size)]

        dfs = []
        for batch in predicate_batches:
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .option("fetchsize", 10000)

            if custom_schema:
                df = df.option("customSchema", custom_schema)

            if extra_options:
                for k, v in extra_options.items():
                    df = df.option(k, v)

            dfs.append(df.load(predicates=batch))

        return reduce(lambda a, b: a.union(b), dfs)
