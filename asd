from pyspark.sql import SparkSession
from typing import Tuple, Dict, Optional, Union, List
import math
import logging
import os
from functools import lru_cache
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PartitionStrategy(str, Enum):
    RANGE = "range"
    HASH = "hash"
    MOD = "mod"

@lru_cache(maxsize=32)
def get_table_stats(spark: SparkSession, table_name: str, jdbc_url: str, jdbc_props: Dict) -> Dict:
    """
    Fetches and caches basic table statistics.
    
    Args:
        spark: Existing SparkSession
        table_name: Name of the Teradata table
        jdbc_url: Teradata JDBC URL
        jdbc_props: JDBC connection properties
    
    Returns:
        Dictionary containing table statistics
    """
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
            logger.warning(f"Could not fetch column metadata: {e}")
            columns_info = []
        
        return {
            "total_rows": total_rows,
            "columns_info": columns_info
        }
    except Exception as e:
        logger.error(f"Error fetching table stats: {e}")
        raise RuntimeError(f"Failed to fetch table statistics: {e}")

def get_column_bounds(spark: SparkSession, table_name: str, partition_column: str, 
                     jdbc_url: str, jdbc_props: Dict) -> Tuple[Union[int, float, str], Union[int, float, str]]:
    """
    Get the min and max values for a column to establish partition boundaries.
    
    Args:
        spark: SparkSession
        table_name: Table name
        partition_column: Column to get bounds for
        jdbc_url: JDBC URL
        jdbc_props: JDBC properties
    
    Returns:
        Tuple of (min_value, max_value)
    """
    try:
        min_query = f"(SELECT MIN({partition_column}) AS min_val FROM {table_name}) AS min_tmp"
        max_query = f"(SELECT MAX({partition_column}) AS max_val FROM {table_name}) AS max_tmp"
        
        min_df = spark.read.jdbc(url=jdbc_url, table=min_query, properties=jdbc_props)
        max_df = spark.read.jdbc(url=jdbc_url, table=max_query, properties=jdbc_props)
        
        min_val = min_df.collect()[0]["min_val"]
        max_val = max_df.collect()[0]["max_val"]
        
        return min_val, max_val
    except Exception as e:
        logger.error(f"Error getting column bounds for {partition_column}: {e}")
        raise RuntimeError(f"Failed to get column bounds: {e}")

def estimate_row_size(spark: SparkSession, table_name: str, jdbc_url: str, jdbc_props: Dict) -> int:
    """
    Estimate the average size of a row in bytes.
    
    Args:
        spark: SparkSession
        table_name: Table name
        jdbc_url: JDBC URL
        jdbc_props: JDBC properties
    
    Returns:
        Estimated row size in bytes
    """
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
        logger.warning(f"Could not estimate row size: {e}, using default of 1KB")
        return 1024  # Default: 1KB per row

def analyze_skew(spark: SparkSession, table_name: str, primary_index_col: str, 
                jdbc_url: str, jdbc_props: Dict) -> Dict:
    """
    Analyze data skew based on Primary Index distribution.
    
    Args:
        spark: SparkSession
        table_name: Table name
        primary_index_col: Primary index column
        jdbc_url: JDBC URL
        jdbc_props: JDBC properties
    
    Returns:
        Dictionary with skew statistics
    """
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
        logger.error(f"Error analyzing skew: {e}")
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
def recommend_partition_column(spark: SparkSession, table_name: str, jdbc_url: str, 
                             jdbc_props: Dict, primary_index_col: str) -> List[str]:
    """
    Analyze columns and recommend suitable partition columns.
    
    Args:
        spark: SparkSession
        table_name: Table name
        jdbc_url: JDBC URL
        jdbc_props: JDBC properties
        primary_index_col: Primary index column
    
    Returns:
        List of recommended partition columns in order of suitability
    """
    try:
        # Query for column cardinality and distribution
        cardinality_query = f"""
        (WITH sample AS (
            SELECT * FROM {table_name}
            SAMPLE 1000
         )
         SELECT 
            c.ColumnName,
            c.ColumnType,
            COUNT(DISTINCT s.{primary_index_col}) AS approx_rows,
            COUNT(DISTINCT CAST(s.{{column}} AS VARCHAR(100))) AS distinct_values,
            CASE WHEN c.ColumnType LIKE '%INT%' OR c.ColumnType LIKE '%NUMERIC%' OR c.ColumnType LIKE '%DECIMAL%' 
                 THEN 'NUMERIC'
                 WHEN c.ColumnType LIKE '%DATE%' OR c.ColumnType LIKE '%TIME%' 
                 THEN 'DATETIME'
                 ELSE 'STRING' 
            END AS data_type
         FROM DBC.ColumnsV c
         JOIN sample s ON 1=1
         WHERE c.DatabaseName = '{jdbc_props.get('database', '')}'
           AND c.TableName = '{table_name.split('.')[-1]}'
         GROUP BY 1, 2
         ORDER BY COUNT(DISTINCT CAST(s.{{column}} AS VARCHAR(100))) DESC
        ) AS tmp
        """
        
        # This might not work directly - in real implementation, 
        # we'd need to handle this in a loop for each column
        # For simplicity, we'll return the primary index column plus some common partition columns
        
        candidate_columns = [primary_index_col]
        
        # Add common partition columns if they differ from primary index
        common_partition_cols = ["date", "datetime", "created_at", "transaction_date", 
                               "year", "month", "day", "region", "country"]
        
        # Get column info to filter candidates
        table_stats = get_table_stats(spark, table_name, jdbc_url, jdbc_props)
        if "columns_info" in table_stats:
            column_names = [col["ColumnName"].lower() for col in table_stats["columns_info"]]
            for col in common_partition_cols:
                if col.lower() in column_names and col.lower() != primary_index_col.lower():
                    candidate_columns.append(col)
        
        return candidate_columns
    except Exception as e:
        logger.warning(f"Error recommending partition columns: {e}")
        return [primary_index_col]  # Fall back to primary index

def find_optimal_partitions_pyspark(
    spark: SparkSession,
    table_name: str,
    primary_index_col: str,
    jdbc_url: str,
    username: str,
    password: str,
    database: str,
    desired_rows_per_partition: int = 1000000,
    max_partitions: Optional[int] = None,
    partition_column: Optional[str] = None,
    partition_strategy: PartitionStrategy = PartitionStrategy.RANGE,
    verbose: bool = False,
    estimated_executor_memory_mb: int = 4096,
    memory_overhead_factor: float = 0.3
) -> Tuple[int, Dict, Optional[Dict]]:
    """
    Calculate the optimal number of partitions for fetching data from Teradata using PySpark.
    
    Args:
        spark (SparkSession): Existing Spark session.
        table_name (str): Name of the Teradata table.
        primary_index_col (str): Primary Index column for skew analysis.
        jdbc_url (str): Teradata JDBC URL (e.g., "jdbc:teradata://host/DATABASE=database").
        username (str): Teradata username.
        password (str): Teradata password.
        database (str): Database name.
        desired_rows_per_partition (int): Target number of rows per partition (default: 1M).
        max_partitions (int, optional): Upper limit on partitions (e.g., Spark executor cores).
        partition_column (str, optional): Column to use for partitioning (defaults to primary_index_col).
        partition_strategy (PartitionStrategy): Strategy for partitioning (range, hash, or mod).
        verbose (bool): Whether to log detailed progress information.
        estimated_executor_memory_mb (int): Available memory per executor in MB.
        memory_overhead_factor (float): Factor for memory overhead (0.3 = 30% overhead).
    
    Returns:
        Tuple[int, Dict, Optional[Dict]]: (optimal partition count, metadata dictionary, partition boundaries if applicable)
    """
    if verbose:
        logger.info(f"Starting partition optimization for table: {table_name}")
    
    # JDBC connection properties with secure handling
    jdbc_props = {
        "user": username,
        "password": password,
        "driver": "com.teradata.jdbc.TeraDriver",
        "database": database
    }
    
    try:
        # Step 1: Load basic table stats
        if verbose:
            logger.info("Step 1/6: Fetching table statistics...")
        
        table_stats = get_table_stats(spark, table_name, jdbc_url, jdbc_props)
        total_rows = table_stats["total_rows"]
        
        if total_rows == 0:
            logger.warning(f"Table {table_name} appears to be empty")
            return 1, {"total_rows": 0, "optimal_partitions": 1}, None
        
        # Step 2: Determine the best partition column if not provided
        if partition_column is None:
            if verbose:
                logger.info("Step 2/6: Selecting optimal partition column...")
            
            recommended_cols = recommend_partition_column(spark, table_name, jdbc_url, jdbc_props, primary_index_col)
            partition_column = recommended_cols[0]  # Use the first recommended column
            
            if verbose:
                logger.info(f"Selected partition column: {partition_column}")
        else:
            if verbose:
                logger.info(f"Using provided partition column: {partition_column}")
        
        # Step 3: Analyze skew based on Primary Index distribution
        if verbose:
            logger.info("Step 3/6: Analyzing data skew...")
        
        skew_data = analyze_skew(spark, table_name, primary_index_col, jdbc_url, jdbc_props)
        
        # Step 4: Estimate row size for memory calculations
        if verbose:
            logger.info("Step 4/6: Estimating memory requirements...")
        
        row_size_bytes = estimate_row_size(spark, table_name, jdbc_url, jdbc_props)
        available_memory_bytes = int(estimated_executor_memory_mb * 1024 * 1024 * (1 - memory_overhead_factor))
        max_rows_per_partition_memory = available_memory_bytes // row_size_bytes
        
        # Adjust desired rows based on memory constraints
        adjusted_rows_per_partition = min(desired_rows_per_partition, max_rows_per_partition_memory)
        
        if verbose and adjusted_rows_per_partition < desired_rows_per_partition:
            logger.info(f"Adjusted rows per partition from {desired_rows_per_partition} to " 
                       f"{adjusted_rows_per_partition} due to memory constraints")
            
        # Step 5: Calculate initial partition estimate
        if verbose:
            logger.info("Step 5/6: Calculating optimal partitions...")
        
        initial_partitions = math.ceil(total_rows / adjusted_rows_per_partition)
        
        # Adjust for skew
        optimal_partitions = initial_partitions
        if skew_data["high_skew"]:
            # Increase partitions to mitigate skew, capped at 2x initial estimate
            skew_adjustment = min(2, 1 + (skew_data["skew_factor_percent"] / 100))
            optimal_partitions = int(initial_partitions * skew_adjustment)
            
            if verbose:
                logger.info(f"Adjusting partitions for high skew (factor: {skew_data['skew_factor_percent']:.2f}%)")
        
        # Step 6: Adjust for Spark cluster parallelism
        spark_cores = spark._jsc.sc().getExecutorMemoryStatus().size()  # Number of executor nodes
        default_parallelism = spark.sparkContext.defaultParallelism  # Configured parallelism
        effective_max_partitions = default_parallelism if max_partitions is None else max_partitions
        
        optimal_partitions = min(optimal_partitions, effective_max_partitions)
        optimal_partitions = max(1, optimal_partitions)  # Ensure at least 1 partition
        
        # Step 7: Determine partition boundaries based on strategy
        partition_boundaries = None
        if partition_strategy == PartitionStrategy.RANGE and optimal_partitions > 1:
            if verbose:
                logger.info("Step 6/6: Calculating partition boundaries...")
            
            try:
                min_val, max_val = get_column_bounds(spark, table_name, partition_column, jdbc_url, jdbc_props)
                
                # For numeric or date columns
                if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                    step = (max_val - min_val) / optimal_partitions
                    boundaries = {
                        "lowerBound": min_val,
                        "upperBound": max_val,
                        "numPartitions": optimal_partitions,
                        "columnType": "numeric"
                    }
                else:
                    # For string columns, we can't easily determine boundaries
                    boundaries = {
                        "columnType": "string",
                        "partitionMethod": "hash"  # Fallback to hash partitioning for strings
                    }
                
                partition_boundaries = boundaries
            except Exception as e:
                logger.warning(f"Error calculating partition boundaries: {e}")
                partition_boundaries = None
        
        # Compile metadata
        metadata = {
            "total_rows": total_rows,
            "partition_column": partition_column,
            "partition_strategy": partition_strategy,
            "row_size_bytes": row_size_bytes,
            "adjusted_rows_per_partition": adjusted_rows_per_partition,
            "initial_partitions": initial_partitions,
            "spark_cores": spark_cores,
            "default_parallelism": default_parallelism,
            "optimal_partitions": optimal_partitions
        }
        
        # Add skew data to metadata
        metadata.update(skew_data)
        
        if verbose:
            logger.info(f"Optimization complete. Optimal partitions: {optimal_partitions}")
        
        return optimal_partitions, metadata, partition_boundaries
        
    except Exception as e:
        logger.error(f"Error in partition optimization: {e}")
        raise RuntimeError(f"Partition optimization failed: {e}")

def fetch_partitioned_data(
    spark: SparkSession,
    table_name: str,
    jdbc_url: str,
    jdbc_props: Dict,
    partition_column: str,
    num_partitions: int,
    partition_boundaries: Optional[Dict] = None
):
    """
    Fetch data from Teradata using the optimized partitioning scheme.
    
    Args:
        spark: SparkSession
        table_name: Table name
        jdbc_url: JDBC URL
        jdbc_props: JDBC properties
        partition_column: Column to partition on
        num_partitions: Number of partitions
        partition_boundaries: Optional boundaries for range partitioning
    
    Returns:
        DataFrame with the fetched data
    """
    try:
        if partition_boundaries and partition_boundaries.get("columnType") == "numeric":
            # Use range partitioning with specific boundaries
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                column=partition_column,
                lowerBound=partition_boundaries["lowerBound"],
                upperBound=partition_boundaries["upperBound"],
                numPartitions=num_partitions,
                properties=jdbc_props
            )
        else:
            # Use default partitioning (less efficient but works for all column types)
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
        logger.error(f"Error fetching data: {e}")
        raise RuntimeError(f"Data fetch failed: {e}")

# Example usage
if __name__ == "__main__":
    # Get credentials from environment variables or a secure source
    jdbc_url = os.environ.get("TERADATA_JDBC_URL", "jdbc:teradata://your_host/DATABASE=your_database")
    username = os.environ.get("TERADATA_USERNAME", "your_user")
    password = os.environ.get("TERADATA_PASSWORD", "your_password")
    database = os.environ.get("TERADATA_DATABASE", "your_database")
    table_name = os.environ.get("TERADATA_TABLE", "your_table")
    pi_column = os.environ.get("TERADATA_PI_COLUMN", "your_primary_index_column")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TeradataPartitionedFetch") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Optional: Specify a different column for partitioning if PI is too skewed
        partition_col = os.environ.get("PARTITION_COLUMN", None)  # Use None to auto-select
        
        # Get optimal partitioning
        num_partitions, stats, boundaries = find_optimal_partitions_pyspark(
            spark=spark,
            table_name=table_name,
            primary_index_col=pi_column,
            jdbc_url=jdbc_url,
            username=username,
            password=password,
            database=database,
            desired_rows_per_partition=1000000,  # 1M rows per partition
            max_partitions=64,  # Example: cap at 64 partitions
            partition_column=partition_col,
            partition_strategy=PartitionStrategy.RANGE,
            verbose=True
        )
        
        logger.info(f"Optimal Number of Partitions: {num_partitions}")
        logger.info("Metadata:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        # JDBC properties for fetching data
        jdbc_props = {
            "user": username,
            "password": password,
            "driver": "com.teradata.jdbc.TeraDriver",
            "database": database
        }
        
        # Fetch the data using optimized partitioning
        part_col = stats["partition_column"]
        logger.info(f"Fetching data using partition column: {part_col}")
        
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
        logger.info(f"Fetched data with {df.rdd.getNumPartitions()} partitions")
        logger.info(f"Sample data:")
        df.show(5)
        
        # Analyze partition distribution
        partition_sizes = df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
        logger.info(f"Partition sizes: {partition_sizes}")
        
        # Example processing: Count records by partition
        partition_counts = df.rdd.mapPartitionsWithIndex(
            lambda idx, it: [(idx, sum(1 for _ in it))]).toDF(["partition_id", "count"])
        
        logger.info("Partition distribution:")
        partition_counts.show()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        spark.stop()
