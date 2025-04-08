import math

def recommend_jdbc_partitioning_v2(
    total_rows,
    avg_row_kb,
    partition_column_type,
    distinct_count=None,
    min_value=None,
    max_value=None,
    null_count=0, # Added: Number of NULLs in partition column
    max_partitions=128, # Increased default max
    min_partitions=8,
    target_partition_mb=128
):
    """
    Improved universal JDBC partition advisor for Spark, focusing on balancing
    partition size while considering potential range/skew issues.

    Args:
        total_rows (int): Total number of rows in the table.
        avg_row_kb (float): Average size of a row in kilobytes.
        partition_column_type (str): Data type of the partition column
                                     (e.g., 'int', 'bigint', 'long', 'numeric',
                                     'decimal', 'string', 'date', 'timestamp').
        distinct_count (int, optional): Number of distinct non-null values in the
                                        partition column. Defaults to None.
        min_value (any, optional): Minimum non-null value in the partition column.
                                   Required for numeric range partitioning. Defaults to None.
        max_value (any, optional): Maximum non-null value in the partition column.
                                   Required for numeric range partitioning. Defaults to None.
        null_count (int, optional): Number of rows where the partition column is NULL.
                                    Defaults to 0. Helps assess skew.
        max_partitions (int, optional): Absolute maximum number of partitions allowed.
                                        Defaults to 128.
        min_partitions (int, optional): Minimum number of partitions desired.
                                        Defaults to 8.
        target_partition_mb (int, optional): Target size (in MB) for each Spark partition.
                                             Defaults to 128.

    Returns:
        dict: A dictionary containing the recommended partitioning strategy and parameters.
            Possible strategies: 'range', 'custom_predicates', 'single_partition'.
    """

    if total_rows <= 0 or avg_row_kb <= 0:
        return {
            "strategy": "single_partition",
            "numPartitions": 1,
            "reason": "No data or invalid size information.",
            "notes": "Reading with a single partition as table appears empty or size is unknown."
        }

    # Calculate total data size in MB (excluding potential NULL impact for now)
    non_null_rows = total_rows - null_count
    if non_null_rows <= 0:
         return {
            "strategy": "single_partition",
            "numPartitions": 1,
            "reason": "All values in partition column are NULL or table is effectively empty.",
            "notes": "Standard partitioning columns require non-null values. Use a single partition."
        }

    total_data_mb = (non_null_rows * avg_row_kb) / 1024.0

    # Estimate partitions based purely on target size
    if total_data_mb == 0:
        # Avoid division by zero if avg_row_kb is tiny
         size_based_partitions = min_partitions
    else:
        # Ensure at least 1, round up
        size_based_partitions = max(1, math.ceil(total_data_mb / target_partition_mb))

    # Apply general min/max constraints
    size_based_partitions = max(min_partitions, min(size_based_partitions, max_partitions))

    # --- Handle Non-Numeric Types ---
    # Standard Spark range partitioning primarily supports numeric types well.
    numeric_types = ("int", "bigint", "long", "numeric", "decimal", "smallint", "tinyint", "float", "double", "real")
    date_types = ("date", "timestamp")
    string_types = ("string", "varchar", "char", "text") # Add other relevant string types

    effective_type = partition_column_type.lower().split('(')[0] # Get base type like 'decimal' from 'decimal(10,2)'

    if effective_type not in numeric_types:
        notes = [
            f"Partition column type '{partition_column_type}' is not ideal for standard Spark range partitioning.",
            f"Recommended strategy is 'custom_predicates' for better control and balance.",
            "Generate WHERE clauses to divide the data logically.",
            f"Example for DATE: ['date_col >= ''2023-01-01'' AND date_col < ''2023-04-01''', ...]",
            f"Example for STRING (prefix): ['SUBSTR(str_col, 1, 1) = ''A''', 'SUBSTR(str_col, 1, 1) = ''B''', ...]",
            f"Example for STRING (hash/modulo - requires DB function): ['MOD(HASH(str_col), 4) = 0', ...]",
            "Determine predicate boundaries based on data distribution (query value counts/ranges).",
            f"Aim for roughly {target_partition_mb}MB per predicate.",
            f"Consider {size_based_partitions} predicates based on data size estimate."
        ]
        if null_count > 0:
             notes.append(f"Consider a separate predicate for NULL values: ['partition_col IS NULL', ...]")

        return {
            "strategy": "custom_predicates",
            "recommended_predicate_count": size_based_partitions,
            "reason": f"Unsupported or non-optimal type for range partitioning: {partition_column_type}",
            "notes": "\n".join(notes)
        }

    # --- Handle Numeric Types ---
    if min_value is None or max_value is None:
        # Can't use range partitioning without bounds
        return {
            "strategy": "coarse_range", # Still using range, but without bounds check
            "numPartitions": size_based_partitions,
            "reason": "Numeric column, but min/max bounds not provided.",
            "notes": f"Proceeding with {size_based_partitions} partitions based on size estimate. Spark will query min/max itself, but distribution is unknown. Monitor task skew."
        }

    # Validate bounds
    try:
        if float(min_value) >= float(max_value):
             # If min == max, only one value (or range is zero)
             if distinct_count == 1:
                 return {
                     "strategy": "single_partition",
                     "numPartitions": 1,
                     "reason": "Partition column has only one distinct value.",
                     "notes": "Partitioning is ineffective."
                 }
             else:
                 # This case (min>=max but distinct > 1) seems inconsistent, but default to size.
                 return {
                     "strategy": "coarse_range",
                     "numPartitions": size_based_partitions,
                     "reason": f"min_value ({min_value}) >= max_value ({max_value}), but distinct_count ({distinct_count}) > 1. Inconsistency?",
                     "notes": f"Proceeding with {size_based_partitions} partitions based on size estimate. Verify metadata. Monitor task skew."
                 }
    except (ValueError, TypeError):
         return {
            "strategy": "custom_predicates", # Fallback if min/max aren't really numeric
            "recommended_predicate_count": size_based_partitions,
            "reason": f"Could not interpret min/max values ({min_value}, {max_value}) as numeric for type {partition_column_type}.",
            "notes": "Verify min/max values match the column type. Consider using custom predicates."
        }

    # --- Metadata Sanity Checks for Numeric Range ---
    notes = []
    final_np = size_based_partitions

    if distinct_count is not None:
        # Check 1: Very few distinct values
        if distinct_count <= 1:
            return {
                "strategy": "single_partition",
                "numPartitions": 1,
                "reason": "Partition column has only one distinct non-null value.",
                "notes": "Range partitioning is ineffective."
            }

        # Check 2: Number of partitions exceeds distinct values significantly
        # Creating many partitions over a small number of distinct values might be inefficient,
        # as multiple partitions might just query the same small set of values.
        # However, standard range partitioning divides the *value range*, not distinct values.
        # If data is skewed (e.g., 90% of rows have value 1, 10% have value 1000),
        # reducing partitions based on distinct count could worsen skew.
        # So, we only add a note here, rather than changing final_np.
        if final_np > distinct_count:
            notes.append(f"WARNING: Recommended partitions ({final_np}) exceeds distinct values ({distinct_count}). "
                         f"Standard range partitioning divides the value space ({min_value} to {max_value}), "
                         f"which might be acceptable if values are sparse but rows are somewhat distributed. "
                         f"However, if rows are heavily concentrated on a few values, this could lead to "
                         f"inefficiency or skew. Consider 'custom_predicates' if task times are uneven.")

        # Check 3: Sparseness (Distinct values much smaller than potential range size)
        # This requires calculating the range, be mindful of type limits
        try:
            # Attempt range calculation carefully
            range_span = float(max_value) - float(min_value)
            # Heuristic: If range is large but distinct count is small relative to partitions
            # Be careful with floats vs integers here. This check is approximate.
            # Example: Range 0 to 1 billion, but only 100 distinct values.
            # Spark will create partitions spanning large empty ranges.
            if range_span > 0 and distinct_count < final_np and range_span / final_np > distinct_count: # Heuristic for sparsity impact
                 notes.append(f"INFO: Data may be sparse within the range [{min_value}, {max_value}] "
                              f"(Distinct: {distinct_count}, Range Span: {range_span:.2f}, Partitions: {final_np}). "
                              f"Standard range partitioning might create tasks processing empty value ranges. "
                              f"This is usually acceptable but monitor performance.")

        except (OverflowError, ValueError, TypeError):
            notes.append("INFO: Could not accurately assess range span vs distinct count due to value types/magnitudes.")


    # Check 4: High proportion of NULLs
    null_proportion = (null_count / total_rows) if total_rows > 0 else 0
    if null_proportion > 0.2: # If > 20% NULLs
         notes.append(f"WARNING: High proportion of NULLs ({null_proportion:.1%}). "
                      f"NULLs in the partition column ({null_count} rows) are typically handled by Spark in a single task/partition "
                      f"or excluded depending on JDBC driver/options. This can cause skew if null_count is large. "
                      f"Consider 'custom_predicates' to handle NULLs explicitly if needed.")

    # Check 5: Potential for Skew (Cannot be perfectly detected without querying distribution)
    notes.append(f"INFO: Final partition count ({final_np}) is based on balancing estimated data volume ({target_partition_mb}MB/partition).")
    notes.append(f"INFO: Standard range partitioning divides the value range [{min_value}, {max_value}] evenly. "
                 f"This works best if *rows* are somewhat evenly distributed across that value range.")
    notes.append(f"WARNING: If data is heavily skewed (e.g., most rows concentrated in a small part of the value range), "
                 f"standard range partitioning can still lead to unbalanced task sizes. Monitor Spark UI for task duration/shuffle statistics. "
                 f"If significant skew is observed, 'custom_predicates' based on row count distribution (e.g., using percentiles or frequent values) is the most robust solution.")


    return {
        "strategy": "range",
        "numPartitions": final_np,
        "lowerBound": str(min_value), # Pass as strings for Spark options
        "upperBound": str(max_value),
        "reason": f"Numeric column with bounds. Estimated {final_np} partitions based on target size {target_partition_mb}MB.",
        "size_based_estimate": size_based_partitions, # Keep the original purely size-based number for reference
        "notes": "\n".join(notes)
    }

# --- Example Usage ---
print("--- Scenario 1: Ideal Numeric ---")
rec1 = recommend_jdbc_partitioning_v2(
    total_rows=100_000_000,
    avg_row_kb=1.5,
    partition_column_type="int",
    distinct_count=80_000_000,
    min_value=1,
    max_value=100_000_000,
    null_count=1000,
    target_partition_mb=128
)
print(rec1)
# Expected: 'range' strategy, numPartitions around (100M * 1.5 / 1024) / 128 = 1145 (clamped by max_partitions=128 by default)

print("\n--- Scenario 2: Numeric, Few Distinct ---")
rec2 = recommend_jdbc_partitioning_v2(
    total_rows=50_000_000,
    avg_row_kb=0.5,
    partition_column_type="bigint",
    distinct_count=50, # Low distinct count
    min_value=1000,
    max_value=50000,
    null_count=0,
    target_partition_mb=256,
    max_partitions=64
)
print(rec2)
# Expected: 'range' strategy, numPartitions based on size ((50M * 0.5 / 1024) / 256 = 96), clamped by max=64.
# Note about partitions > distinct count.

print("\n--- Scenario 3: String Type ---")
rec3 = recommend_jdbc_partitioning_v2(
    total_rows=200_000_000,
    avg_row_kb=1.0,
    partition_column_type="string",
    # distinct_count, min/max typically not useful for 'custom_predicates'
    target_partition_mb=128,
    max_partitions=100
)
print(rec3)
# Expected: 'custom_predicates' strategy, recommended count based on size ((200M * 1.0 / 1024) / 128 = 1526), clamped by max=100.

print("\n--- Scenario 4: Numeric, Missing Bounds ---")
rec4 = recommend_jdbc_partitioning_v2(
    total_rows=10_000_000,
    avg_row_kb=2.0,
    partition_column_type="decimal(18,0)",
    distinct_count=5_000_000,
    # min_value=None, max_value=None, # Missing
    target_partition_mb=64,
    min_partitions=4,
    max_partitions=32
)
print(rec4)
# Expected: 'coarse_range', numPartitions based on size ((10M * 2.0 / 1024) / 64 = 305), clamped by max=32. Reason: missing bounds.

print("\n--- Scenario 5: High Null Count ---")
rec5 = recommend_jdbc_partitioning_v2(
    total_rows=100_000_000,
    avg_row_kb=1.5,
    partition_column_type="int",
    distinct_count=80_000_000,
    min_value=1,
    max_value=100_000_000,
    null_count=30_000_000, # 30% NULLs
    target_partition_mb=128,
    max_partitions=128
)
print(rec5)
# Expected: 'range', numPartitions based on non-null size ((70M * 1.5 / 1024) / 128 = 802), clamped by max=128. Warning about high null count.

print("\n--- Scenario 6: Small Table ---")
rec6 = recommend_jdbc_partitioning_v2(
    total_rows=500_000,
    avg_row_kb=0.2,
    partition_column_type="int",
    distinct_count=400_000,
    min_value=1,
    max_value=500_000,
    null_count=0,
    target_partition_mb=128,
    min_partitions=8 # Force minimum
)
print(rec6)
# Expected: 'range', numPartitions based on size ((0.5M * 0.2 / 1024) = ~0.1MB total), size_based = 1, but clamped by min_partitions=8.
