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
    extra_options=None
):
    """
    Smart JDBC reader that picks optimal partitioning strategy based on input and cluster config.
    """

    # Default options
    options = {
        "url": jdbc_url,
        "dbtable": table_name,
        "partitionColumn": partition_column,
        "user": user,
        "password": password,
        "fetchsize": "10000",
    }
    if extra_options:
        options.update(extra_options)

    # Safety checks
    if partition_column is None or lower_bound is None or upper_bound is None:
        return {
            "strategy": "custom_predicates",
            "reason": "Missing partitioning metadata. Use manual filters or predicates."
        }

    if distinct_count < 1000 or distinct_count >= total_rows * 0.9 or force_predicates:
        # Too few distinct or almost one-to-one mapping (or forced)
        print("\u26a0\ufe0f Using custom predicates: partition column not suitable for range partitioning.")
        predicates = [f"{partition_column} = {val}" for val in range(lower_bound, upper_bound + 1)]
        return spark.read.jdbc(jdbc_url, table_name, predicates[:max_jdbc_connections], properties={"user": user, "password": password})

    # Pick numPartitions based on cores and DB connection limit
    max_ideal_parts = min(max_jdbc_connections, cluster_cores, distinct_count)
    stride = (upper_bound - lower_bound) // max_ideal_parts
    if stride < 1:
        print("\u26a0\ufe0f Stride too small, falling back to custom predicates.")
        predicates = [f"{partition_column} = {val}" for val in range(lower_bound, upper_bound + 1)]
        return spark.read.jdbc(jdbc_url, table_name, predicates[:max_jdbc_connections], properties={"user": user, "password": password})

    # Final options
    options.update({
        "lowerBound": str(lower_bound),
        "upperBound": str(upper_bound),
        "numPartitions": str(max_ideal_parts)
    })

    print(f"\u2705 Using range partitioning: {max_ideal_parts} partitions, stride ~ {stride}")
    return spark.read.format("jdbc").options(**options).load()
