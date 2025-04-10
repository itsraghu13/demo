from pyspark.sql import SparkSession, functions as F
from concurrent.futures import ThreadPoolExecutor
import math
import time

# Configuration parameters
server = "your_teradata_server"
db = "your_database"
username = "your_username"
password = "your_password"
table = "your_table"
index_column = "your_index_column"  # The integer column we're optimizing for
delta_table_path = "dbfs:/mnt/delta/your_target_table"

# Teradata connection parameters
jdbc_url = f"jdbc:teradata://{server}/database={db},TMODE=TERA"
driver = "com.teradata.jdbc.TeraDriver"

# Performance parameters
chunk_size_rows = 6_000_000  # Target rows per chunk for writing
max_records_per_file = 500_000  # Controls file size in Delta Lake
parallel_extraction_count = 16  # Number of parallel Teradata extractions
max_write_workers = 8  # Number of parallel write processes
num_spark_partitions = 200  # Total Spark partitions

# Step 1: Get metadata about the index column from Teradata with a single query
def get_index_metadata():
    print("Getting index column metadata from Teradata...")
    
    # Direct properties for JDBC connection
    properties = {
        "user": username,
        "password": password,
        "driver": driver
    }
    
    # Efficient metadata query
    metadata_query = f"""
    SELECT 
        MIN({index_column}) as min_val, 
        MAX({index_column}) as max_val,
        COUNT(DISTINCT {index_column}) as distinct_count,
        COUNT(*) as total_count
    FROM {table}
    """
    
    metadata_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({metadata_query}) as metadata",
        properties=properties
    )
    
    metadata = metadata_df.collect()[0]
    print(f"Index column metadata: min={metadata.min_val}, max={metadata.max_val}, " + 
          f"distinct values={metadata.distinct_count}, total rows={metadata.total_count}")
    
    return metadata.min_val, metadata.max_val, metadata.distinct_count, metadata.total_count

# Step 2: Generate optimized extraction ranges based on index distribution
def generate_extraction_ranges(min_val, max_val, distinct_count, total_count):
    print(f"Generating {parallel_extraction_count} extraction ranges...")
    
    # Calculate range size
    range_size = max_val - min_val + 1
    
    # Check if we should do specialized partitioning for skewed data
    if distinct_count < total_count * 0.8:  
        # Data has significant duplication in index values
        # For skewed data, we'd ideally generate ranges based on quantiles
        # But for simplicity, we'll use evenly sized ranges
        step = math.ceil(range_size / parallel_extraction_count)
    else:
        # For uniform data, we can use equally sized ranges
        step = math.ceil(range_size / parallel_extraction_count)
    
    ranges = [(i, min(i + step - 1, max_val)) for i in range(min_val, max_val + 1, step)]
    print(f"Generated {len(ranges)} extraction ranges")
    return ranges

# Step 3: Extract and write data in a single efficient operation per range
def extract_and_write_chunk(range_tuple):
    range_start, range_end = range_tuple
    
    print(f"Starting extraction and processing for range {range_start} to {range_end}")
    
    # Connection properties for Teradata
    properties = {
        "user": username,
        "password": password,
        "driver": driver,
        "fetchSize": "100000",
        "ConnectionRetries": "3",
        "ConnectionRetryInterval": "2000",
        "SessionMode": "NoWait",  # Important for performance
        "TMODE": "TERA"
    }
    
    # Extract data efficiently using predicates
    # This approach lets Teradata optimize the query execution
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("partitionColumn", index_column) \
        .option("lowerBound", range_start) \
        .option("upperBound", range_end) \
        .option("numPartitions", num_spark_partitions // parallel_extraction_count) \
        .options(**properties) \
        .load()
    
    # Optimize the DataFrame by partitioning by index to prepare for writing
    df = df.repartition(max_write_workers, F.col(index_column))
    
    # Calculate sub-ranges for efficient Delta writing
    sub_range_size = range_end - range_start + 1
    
    # Determine optimal number of sub-chunks based on data size
    # We estimate 1 million rows per worker for optimal performance
    partitions_per_worker = 2  # Each worker handles 2 partitions for better resource utilization
    total_partitions = max_write_workers * partitions_per_worker
    sub_step = math.ceil(sub_range_size / total_partitions)
    
    # Generate sub-ranges for parallel writing
    sub_ranges = [(i, min(i + sub_step - 1, range_end)) 
                 for i in range(range_start, range_end + 1, sub_step)]
    
    print(f"Writing {len(sub_ranges)} sub-chunks for range {range_start}-{range_end}")
    
    # Process sub-ranges in parallel using ThreadPoolExecutor
    write_results = []
    
    def write_sub_range(sub_range):
        sub_start, sub_end = sub_range
        print(f"Writing sub-range {sub_start} to {sub_end}")
        
        # Filter data for this sub-range
        sub_df = df.filter((F.col(index_column) >= sub_start) & 
                          (F.col(index_column) <= sub_end))
        
        # Add metadata columns
        sub_df = sub_df.withColumn("extract_timestamp", F.current_timestamp())
        
        # Sort within partitions for optimal Delta Lake file structure
        sub_df = sub_df.sortWithinPartitions(index_column)
        
        try:
            # Write to Delta with optimized settings
            sub_df.write \
                .format("delta") \
                .mode("append") \
                .option("maxRecordsPerFile", max_records_per_file) \
                .option("dataChange", "false") \  # Optimize for bulk inserts
                .save(delta_table_path)
            
            print(f"Completed write for sub-range {sub_start} to {sub_end}")
            return sub_start, sub_end, True
        except Exception as e:
            print(f"Error writing sub-range {sub_start} to {sub_end}: {str(e)}")
            return sub_start, sub_end, False
    
    # Use ThreadPoolExecutor for parallel writing
    with ThreadPoolExecutor(max_workers=max_write_workers) as executor:
        future_results = [executor.submit(write_sub_range, sub_range) 
                         for sub_range in sub_ranges]
        
        for future in future_results:
            write_results.append(future.result())
    
    return range_start, range_end, write_results

# Main execution flow
def extract_and_load_to_delta():
    start_time = time.time()
    print(f"Starting optimized Teradata to Delta extraction for table {table}")
    
    try:
        # Get metadata in a single query
        min_val, max_val, distinct_count, total_count = get_index_metadata()
        
        # Generate extraction ranges
        ranges = generate_extraction_ranges(min_val, max_val, distinct_count, total_count)
        
        # Process each range in parallel
        results = []
        with ThreadPoolExecutor(max_workers=parallel_extraction_count) as executor:
            futures = [executor.submit(extract_and_write_chunk, range_tuple) 
                      for range_tuple in ranges]
            
            for future in futures:
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error processing chunk: {str(e)}")
        
        # Optimize the Delta table after all writes are complete
        print("Optimizing Delta table...")
        try:
            # Z-order by index column for optimized query performance
            spark.sql(f"OPTIMIZE delta.`{delta_table_path}` ZORDER BY ({index_column})")
            
            # Vacuum the table to remove old files
            spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN 0 HOURS")
        except Exception as e:
            print(f"Error during table optimization: {str(e)}")
        
        # Get the final count from Delta table
        final_count = spark.read.format("delta").load(delta_table_path).count()
        
        # Calculate statistics
        end_time = time.time()
        duration_minutes = (end_time - start_time) / 60
        rows_per_minute = final_count / duration_minutes if duration_minutes > 0 else 0
        
        print(f"Completed Teradata to Delta extraction")
        print(f"Total rows in Delta table: {final_count}")
        print(f"Total duration: {duration_minutes:.2f} minutes")
        print(f"Average write rate: {rows_per_minute:.0f} rows/minute")
        
        return results, final_count
    except Exception as e:
        print(f"Error in extraction process: {str(e)}")
        raise

# Helper function to generate a session with right configs
def create_optimized_spark_session():
    spark = SparkSession.builder \
        .appName("Teradata to Delta Optimized ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.sql.shuffle.partitions", num_spark_partitions) \
        .config("spark.default.parallelism", num_spark_partitions) \
        .config("spark.databricks.io.cache.enabled", "true") \
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
        .config("spark.databricks.delta.optimize.zorderCols", index_column) \
        .getOrCreate()
    
    return spark

# Execute the process
if __name__ == "__main__":
    spark = create_optimized_spark_session()
    results, final_count = extract_and_load_to_delta()
    print(f"Process complete. Final row count: {final_count}")
