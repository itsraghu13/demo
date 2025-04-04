def recommend_jdbc_partitioning(
    total_rows,
    avg_row_kb,
    partition_column_type,
    distinct_count=None,
    min_value=None,
    max_value=None,
    max_partitions=64,
    min_partitions=8,
    target_partition_mb=128
):
    """
    Universal JDBC partition advisor for Spark.
    """

    total_data_mb = (total_rows * avg_row_kb) / 1024
    size_based_partitions = max(min_partitions, int(total_data_mb / target_partition_mb))

    if partition_column_type.lower() not in ("int", "bigint", "long", "numeric"):
        return {
            "strategy": "custom_predicates",
            "reason": f"Unsupported type: {partition_column_type}",
            "note": "Use predicates with string/timestamp/date types"
        }

    if distinct_count is None or min_value is None or max_value is None:
        return {
            "strategy": "coarse_range",
            "numPartitions": min(size_based_partitions, max_partitions),
            "reason": "Lacking metadata, falling back to size-based estimate"
        }

    if distinct_count < 2 or distinct_count >= total_rows:
        return {
            "strategy": "custom_predicates",
            "reason": "Too few or too many distinct values",
        }

    # Now do skew-stride check
    best_np = None
    best_stride = None
    min_skew = float('inf')

    for np in range(min_partitions, min(max_partitions, distinct_count) + 1):
        stride = distinct_count / np
        skew = abs(stride - round(stride))

        if skew < min_skew:
            min_skew = skew
            best_np = np
            best_stride = stride

    # Final numPartitions = min(best match and size-based)
    final_np = min(best_np, size_based_partitions, max_partitions)

    return {
        "strategy": "range",
        "numPartitions": final_np,
        "stride": round(best_stride),
        "skew": round(min_skew, 4),
        "size_based_estimate": size_based_partitions,
        "notes": "OK to proceed unless values are highly skewed or sparse/null"
    }
