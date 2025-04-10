# Teradata Partition Optimization for Databricks
from functools import lru_cache
import math

@lru_cache(maxsize=32)
def get_table_stats(spark, table_name, jdbc_url, jdbc_props):
    """Fetches and caches basic table statistics."""
    try:
        # Get total rows
        total_rows_query = f"(SELECT COUNT(*) AS total_rows FROM {table_name}) AS tmp"
        total_rows_df = spark.read.jdbc(url=jdbc_url, table=total_rows_query, properties=jdbc_props)
        total_rows = total_rows_df.collect()[0]["total_rows"]
        
        # Get column information
        column_info_query = f"""
        (SELECT 
            ColumnName, 
            ColumnType,
            CASE WHEN ColumnType LIKE '%INT%' OR ColumnType LIKE '%NUMERIC%' OR ColumnType LIKE '%DECIMAL%' 
                 THEN 'NUMERIC'
                 WHEN ColumnType LIKE '%DATE%' OR ColumnType LIKE '%TIME%' 
                 THEN 'DATETIME'
                 ELSE 'STRING' 
            END AS DataCategory
         FROM DBC.ColumnsV
         WHERE DatabaseName = '{jdbc_props.get('database', '')}' 
           AND TableName = '{table_name.split('.')[-1]}'
        ) AS tmp
        """
        
        try:
            columns_df = spark.read.jdbc(url=jdbc_url, table=column_info_query, properties=jdbc_props)
            columns_info = columns_df.collect()
        except Exception as e:
            print(f"Could not fetch column metadata: {e}")
            columns_info = []
        
        return {
            "total_rows": total_rows,
            "columns_info": columns_info
        }
    except Exception as e:
        print(f"Error fetching table stats: {e}")
        raise RuntimeError(f"Failed to fetch table statistics: {e}")

def get_column_bounds(spark, table_name, partition_column, jdbc_url, jdbc_props):
    """Get the min and max values for a column to establish partition boundaries."""
    try:
        # First, determine column type
        column_type_query = f"""
        (SELECT ColumnType 
         FROM DBC.ColumnsV
         WHERE DatabaseName = '{jdbc_props.get('database', '')}'
           AND TableName = '{table_name.split('.')[-1]}'
           AND ColumnName = '{partition_column}'
        ) AS tmp
        """
        
        try:
            col_type_df = spark.read.jdbc(url=jdbc_url, table=column_type_query, properties=jdbc_props)
            column_type = col_type_df.collect()[0]["ColumnType"].upper()
        except Exception as e:
            print(f"Could not determine column type: {e}")
            column_type = "UNKNOWN"
            
        # For numeric or date columns
        if ("INT" in column_type or 
            "NUMERIC" in column_type or 
            "DECIMAL" in column_type or 
            "FLOAT" in column_type or
            "REAL" in column_type or
            "DOUBLE" in column_type):
            
            min_query = f"(SELECT MIN({partition_column}) AS min_val FROM {table_name}) AS min_tmp"
            max_query = f"(SELECT MAX({partition_column}) AS max_val FROM {table_name}) AS max_tmp"
            
            min_df = spark.read.jdbc(url=jdbc_url, table=min_query, properties=jdbc_props)
            max_df = spark.read.jdbc(url=jdbc_url, table=max_query, properties=jdbc_props)
            
            min_val = min_df.collect()[0]["min_val"]
            max_val = max_df.collect()[0]["max_val"]
            
            # Ensure we don't have null or same values that would cause partitioning issues
            if min_val is None or max_val is None or min_val == max_val:
                if min_val == max_val:
                    print(f"Warning: Min and max values are the same ({min_val}). Adjusting for partitioning.")
                    # Adjust the bounds slightly to ensure partitioning works
                    if isinstance(min_val, (int, float)):
                        min_val = min_val - 1
                        max_val = max_val + 1
                    else:
                        # For numeric that isn't int/float (rare)
                        return None, None, "non_numeric"
                else:
                    return None, None, "null_values"
                
            return min_val, max_val, "numeric"
        
        elif "DATE" in column_type or "TIME" in column_type:
            # For date/time columns, we'll also use min/max but handle differently
            min_query = f"(SELECT MIN(CAST({partition_column} AS TIMESTAMP(0))) AS min_val FROM {table_name}) AS min_tmp"
            max_query = f"(SELECT MAX(CAST({partition_column} AS TIMESTAMP(0))) AS max_val FROM {table_name}) AS max_tmp"
            
            min_df = spark.read.jdbc(url=jdbc_url, table=min_query, properties=jdbc_props)
            max_df = spark.read.jdbc(url=jdbc_url, table=max_query, properties=jdbc_props)
            
            min_val = min_df.collect()[0]["min_val"]
            max_val = max_df.collect()[0]["max_val"]
            
            if min_val is None or max_val is None or min_val == max_val:
                return None, None, "date_issue"
            
            # For JDBC partitioning with dates, we need to convert to epoch or another numeric format
            # This is a simplified approach - actual implementation might differ
            min_query = f"""
            (SELECT EXTRACT(EPOCH FROM CAST({partition_column} AS TIMESTAMP(0))) AS min_val 
             FROM {table_name} 
             WHERE {partition_column} = (SELECT MIN({partition_column}) FROM {table_name})
             LIMIT 1) AS min_tmp
            """
            max_query = f"""
            (SELECT EXTRACT(EPOCH FROM CAST({partition_column} AS TIMESTAMP(0))) AS max_val 
             FROM {table_name} 
             WHERE {partition_column} = (SELECT MAX({partition_column}) FROM {table_name})
             LIMIT 1) AS max_tmp
            """
            
            try:
                min_df = spark.read.jdbc(url=jdbc_url, table=min_query, properties=jdbc_props)
                max_df = spark.read.jdbc(url=jdbc_url, table=max_query, properties=jdbc_props)
                
                min_epoch = min_df.collect()[0]["min_val"]
                max_epoch = max_df.collect()[0]["max_val"]
                
                return min_epoch, max_epoch, "datetime"
            except Exception as e:
                print(f"Error converting dates to epoch: {e}")
                return None, None, "date_conversion_error"
        else:
            # For string columns and other non-numeric types, return None
            print(f"Column {partition_column} has type {column_type} which isn't suitable for range partitioning")
            return None, None, "non_numeric"
            
    except Exception as e:
        print(f"Error getting column bounds for {partition_column}: {e}")
        return None, None, "error"

def estimate_row_size(spark, table_name, jdbc_url, jdbc_props):
    """Estimate the average size of a row in bytes."""
    try:
        # This query works for Teradata to estimate row size
        size_query = f"""
        (SELECT 
            CAST(SUM(ColumnLength) AS FLOAT) / COUNT(*) AS avg_row_size_bytes
         FROM DBC.ColumnsV
         WHERE DatabaseName = '{jdbc_props.get('database', '')}'
           AND TableName = '{table_name.split('.')[-1]}'
        ) AS tmp
        """
        
        size_df = spark.read.jdbc(url=jdbc_url, table=size_query, properties=jdbc_props)
        row_size = size_df.collect()[0]["avg_row_size_bytes"]
        
        # If we couldn't get the size, use a default
        if row_size is None or row_size < 100:
            row_size = 1024  # Conservative default: 1KB per row
            
        return int(row_size)
    except Exception as e:
        print(f"Could not estimate row size: {e}, using default of 1KB")
        return 1024  # Default: 1KB per row

def analyze_skew(spark, table_name, primary_index_col, jdbc_url, jdbc_props):
    """Analyze data skew based on Primary Index distribution."""
    try:
        # Get table stats
        stats = get_table_stats(spark, table_name, jdbc_url, jdbc_props)
        total_rows = stats["total_rows"]
        
        # Analyze distribution across AMPs
        skew_query = f"""
            (SELECT 
                HASHAMP(HASHBUCKET(HASHROW({primary_index_col}))) AS amp_no,
                COUNT(*) AS row_count
            FROM {table_name}
            GROUP BY 1) AS tmp
        """
        
        skew_df = spark.read.jdbc(url=jdbc_url, table=skew_query, properties=jdbc_props)
        skew_data = skew_df.collect()
        
        # Calculate skew metrics
        num_amps = len(skew_data)
        if num_amps == 0:
            raise ValueError(f"No AMP data returned for table {table_name}")
            
        avg_rows_per_amp = total_rows / num_amps
        max_rows_per_amp = max(row["row_count"] for row in skew_data)
        min_rows_per_amp = min(row["row_count"] for row in skew_data)
        
        # Calculate skew factor as percentage deviation from average
        skew_factor = ((max_rows_per_amp - avg_rows_per_amp) / avg_rows_per_amp) * 100 if avg_rows_per_amp > 0 else 0
        
        # Calculate coefficient of variation for more accurate skew measurement
        variance = sum((row["row_count"] - avg_rows_per_amp) ** 2 for row in skew_data) / num_amps
        std_dev = math.sqrt(variance)
        cv = (std_dev / avg_rows_per_amp) * 100 if avg_rows_per_amp > 0 else 0
        
        return {
            "num_amps": num_amps,
            "total_rows": total_rows,
            "avg_rows_per_amp": avg_rows_per_amp,
            "max_rows_per_amp": max_rows_per_amp,
            "min_rows_per_amp": min_rows_per_amp,
            "skew_factor_percent": skew_factor,
            "coefficient_of_variation": cv,
            "high_skew": skew_factor > 50 or cv > 50
        }
    except Exception as e:
        print(f"Error analyzing skew: {e}")
        # Return default values indicating unknown skew
        return {
            "num_amps": 1,
            "total_rows": stats.get("total_rows", 0),
            "avg_rows_per_amp": stats.get("total_rows", 0),
            "max_rows_per_amp": stats.get("total_rows", 0),
            "min_rows_per_amp": 0,
            "skew_factor_percent": 0,
            "coefficient_of_variation": 0,
            "high_skew": False
        }

@lru_cache(maxsize=32)
def recommend_partition_column(spark, table_name, jdbc_url, jdbc_props, primary_index_col):
    """Analyze columns and recommend suitable partition columns."""
    try:
        # Get column info to filter candidates
        table_stats = get_table_stats(spark, table_name, jdbc_url, jdbc_props)
        
        candidate_columns = [primary_index_col]
        
        # Add common partition columns if they differ from primary index
        common_partition_cols = ["date", "datetime", "created_at", "transaction_date", 
                               "year", "month", "day", "region", "country"]
        
        if "columns_info" in table_stats:
            column_names = [col["ColumnName"].lower() for col in table_stats["columns_info"]]
            column_types = {col["ColumnName"].lower(): col["DataCategory"] for col in table_stats["columns_info"]}
            
            # First priority: numeric columns (better for range partitioning)
            for col_name in column_names:
                if col_name != primary_index_col.lower() and column_types.get(col_name) == "NUMERIC":
                    candidate_columns.append(col_name)
                    
            # Second priority: date columns
            for col_name in column_names:
                if col_name != primary_index_col.lower() and column_types.get(col_name) == "DATETIME":
                    candidate_columns.append(col_name)
                    
            # Third priority: common partition columns
            for col in common_partition_cols:
                if col.lower() in column_names and col.lower() != primary_index_col.lower():
                    if col.lower() not in candidate_columns:
                        candidate_columns.append(col.lower())
        
        return candidate_columns
    except Exception as e:
        print(f"Error recommending partition columns: {e}")
        return [primary_index_col]  # Fall back to primary index

def find_optimal_partitions(
    spark,
    table_name,
    primary_index_col,
    jdbc_url,
    username,
    password,
    database,
    desired_rows_per_partition=1000000,
    max_partitions=None,
    partition_column=None,
    partition_strategy="range",
    estimated_executor_memory_mb=4096,
    memory_overhead_factor=0.3
):
    """
    Calculate the optimal number of partitions for fetching data from Teradata using PySpark.
    """
    print(f"Starting partition optimization for table: {table_name}")
    
    # JDBC connection properties
    jdbc_props = {
        "user": username,
        "password": password,
        "driver": "com.teradata.jdbc.TeraDriver",
        "database": database
    }
    
    try:
        # Step 1: Load basic table stats
        print("Fetching table statistics...")
        
        table_stats = get_table_stats(spark, table_name, jdbc_url, jdbc_props)
        total_rows = table_stats["total_rows"]
        
        if total_rows == 0:
            print(f"Table {table_name} appears to be empty")
            return 1, {"total_rows": 0, "optimal_partitions": 1}, None
        
        # Step 2: Determine the best partition column if not provided
        if partition_column is None:
            print("Selecting optimal partition column...")
            
            recommended_cols = recommend_partition_column(spark, table_name, jdbc_url, jdbc_props, primary_index_col)
            partition_column = recommended_cols[0]  # Use the first recommended column
            
            print(f"Selected partition column: {partition_column}")
        else:
            print(f"Using provided partition column: {partition_column}")
        
        # Step 3: Analyze skew based on Primary Index distribution
        print("Analyzing data skew...")
        
        skew_data = analyze_skew(spark, table_name, primary_index_col, jdbc_url, jdbc_props)
        
        # Step 4: Estimate row size for memory calculations
        print("Estimating memory requirements...")
        
        row_size_bytes = estimate_row_size(spark, table_name, jdbc_url, jdbc_props)
        available_memory_bytes = int(estimated_executor_memory_mb * 1024 * 1024 * (1 - memory_overhead_factor))
        max_rows_per_partition_memory = available_memory_bytes // row_size_bytes
        
        # Adjust desired rows based on memory constraints
        adjusted_rows_per_partition = min(desired_rows_per_partition, max_rows_per_partition_memory)
        
        if adjusted_rows_per_partition < desired_rows_per_partition:
            print(f"Adjusted rows per partition from {desired_rows_per_partition} to " 
                  f"{adjusted_rows_per_partition} due to memory constraints")
            
        # Step 5: Calculate initial partition estimate
        print("Calculating optimal partitions...")
        
        initial_partitions = math.ceil(total_rows / adjusted_rows_per_partition)
        
        # Adjust for skew
        optimal_partitions = initial_partitions
        if skew_data["high_skew"]:
            # Increase partitions to mitigate skew, capped at 2x initial estimate
            skew_adjustment = min(2, 1 + (skew_data["skew_factor_percent"] / 100))
            optimal_partitions = int(initial_partitions * skew_adjustment)
            
            print(f"Adjusting partitions for high skew (factor: {skew_data['skew_factor_percent']:.2f}%)")
        
        # Step 6: Adjust for Spark cluster parallelism
        default_parallelism = spark.sparkContext.defaultParallelism  # Configured parallelism
        effective_max_partitions = default_parallelism if max_partitions is None else max_partitions
        
        optimal_partitions = min(optimal_partitions, effective_max_partitions)
        optimal_partitions = max(1, optimal_partitions)  # Ensure at least 1 partition
        
        # Step 7: Determine partition boundaries based on strategy
        partition_boundaries = None
        column_type = None
        
        if partition_strategy == "range" and optimal_partitions > 1:
            print("Calculating partition boundaries...")
            
            try:
                min_val, max_val, column_type = get_column_bounds(spark, table_name, partition_column, jdbc_url, jdbc_props)
                
                if min_val is not None and max_val is not None:
                    # Make sure bounds are not identical to avoid partitioning errors
                    if min_val == max_val:
                        if isinstance(min_val, (int, float)):
                            min_val -= 1
                            max_val += 1
                    
                    # For numeric columns with proper bounds
                    if column_type in ["numeric", "datetime"]:
                        boundaries = {
                            "lowerBound": min_val,
                            "upperBound": max_val,
                            "numPartitions": optimal_partitions,
                            "columnType": column_type
                        }
                        partition_boundaries = boundaries
                    else:
                        print(f"Column {partition_column} is not suitable for range partitioning")
                        partition_boundaries = {
                            "columnType": column_type,
                            "partitionMethod": "hash"  # Fallback to hash partitioning
                        }
                else:
                    print(f"Could not determine bounds for {partition_column}, using hash partitioning")
                    partition_boundaries = {
                        "columnType": column_type or "unknown",
                        "partitionMethod": "hash"  # Fallback to hash partitioning
                    }
            except Exception as e:
                print(f"Error calculating partition boundaries: {e}")
                partition_boundaries = None
        
        # Compile metadata
        metadata = {
            "total_rows": total_rows,
            "partition_column": partition_column,
            "partition_strategy": partition_strategy,
            "column_type": column_type,
            "row_size_bytes": row_size_bytes,
            "adjusted_rows_per_partition": adjusted_rows_per_partition,
            "initial_partitions": initial_partitions,
            "default_parallelism": default_parallelism,
            "optimal_partitions": optimal_partitions
        }
        
        # Add skew data to metadata
        metadata.update(skew_data)
        
        print(f"Optimization complete. Optimal partitions: {optimal_partitions}")
        
        return optimal_partitions, metadata, partition_boundaries
        
    except Exception as e:
        print(f"Error in partition optimization: {e}")
        raise RuntimeError(f"Partition optimization failed: {e}")

def fetch_partitioned_data(
    spark,
    table_name,
    jdbc_url,
    jdbc_props,
    partition_column,
    num_partitions,
    partition_boundaries=None
):
    """
    Fetch data from Teradata using the optimized partitioning scheme.
    """
    if num_partitions <= 1:
        print("Using single partition load (no partitioning)")
        return spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_props)
    
    try:
        if (partition_boundaries and 
            partition_boundaries.get("columnType") in ["numeric", "datetime"] and
            "lowerBound" in partition_boundaries and 
            "upperBound" in partition_boundaries):
            
            # Use range partitioning with specific boundaries
            lower_bound = partition_boundaries["lowerBound"]
            upper_bound = partition_boundaries["upperBound"]
            
            # Ensure the bounds are valid and not equal
            if lower_bound == upper_bound:
                if isinstance(lower_bound, (int, float)):
                    lower_bound -= 1
                    upper_bound += 1
                else:
                    # Fallback for unusual cases
                    print("Invalid bounds (equal values), using hash partitioning instead")
                    df = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", table_name) \
                        .option("user", jdbc_props["user"]) \
                        .option("password", jdbc_props["password"]) \
                        .option("driver", jdbc_props["driver"]) \
                        .option("numPartitions", num_partitions) \
                        .load()
                    return df
            
            print(f"Using range partitioning on {partition_column}")
            print(f"Lower bound: {lower_bound}, Upper bound: {upper_bound}, Partitions: {num_partitions}")
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=jdbc_props
            )
        else:
            # Use default partitioning (less efficient but works for all column types)
            print(f"Using hash partitioning with {num_partitions} partitions")
            df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_props["user"]) \
                .option("password", jdbc_props["password"]) \
                .option("driver", jdbc_props["driver"]) \
                .option("numPartitions", num_partitions) \
                .load()
        
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise RuntimeError(f"Data fetch failed: {e}")

# Example usage in Databricks
def load_teradata_table(spark, jdbc_url, username, password, database, table_name, pi_column, partition_col=None):
    """
    Load a Teradata table with optimized partitioning for Databricks
    """
    # Get optimal partitioning
    num_partitions, stats, boundaries = find_optimal_partitions(
        spark=spark,
        table_name=table_name,
        primary_index_col=pi_column,
        jdbc_url=jdbc_url,
        username=username,
        password=password,
        database=database,
        desired_rows_per_partition=1000000,  # 1M rows per partition
        max_partitions=64,  # Example: cap at 64 partitions
        partition_column=partition_col
    )
    
    print(f"Optimal Number of Partitions: {num_partitions}")
    print("Metadata:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # JDBC properties for fetching data
    jdbc_props = {
        "user": username,
        "password": password,
        "driver": "com.teradata.jdbc.TeraDriver",
        "database": database
    }
    
    # Fetch the data using optimized partitioning
    part_col = stats["partition_column"]
    print(f"Fetching data using partition column: {part_col}")
    
    df = fetch_partitioned_data(
        spark=spark,
        table_name=table_name,
        jdbc_url=jdbc_url,
        jdbc_props=jdbc_props,
        partition_column=part_col,
        num_partitions=num_partitions,
        partition_boundaries=boundaries
    )
    
    # Show sample data and partition distribution
    print(f"Fetched data with {df.rdd.getNumPartitions()} partitions")
    print(f"Sample data:")
    df.show(5)
    
    return df
