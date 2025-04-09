from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
import math

def write_delta_in_chunks(
    df, 
    index_column, 
    output_path, 
    chunk_size_rows=6_000_000,  # total rows per chunk (~1 GB depending on row size)
    max_records_per_file=500_000,  # rows per individual output file
    parallel=True,
    max_workers=8,
    save_mode="append",
    add_chunk_id=False
):
    # 1. Get min, max of index column
    stats = df.selectExpr(
        f"min({index_column}) as min_val", 
        f"max({index_column}) as max_val"
    ).collect()[0]
    min_val, max_val = stats["min_val"], stats["max_val"]
    
    total_rows = df.count()
    print(f"📊 Total rows: {total_rows}, index_column range: {min_val} to {max_val}")
    
    # 2. Estimate number of chunks
    total_chunks = math.ceil(total_rows / chunk_size_rows)
    step = math.ceil((max_val - min_val + 1) / total_chunks)
    
    # 3. Create chunk ranges
    ranges = [(i, min(i + step - 1, max_val)) for i in range(min_val, max_val + 1, step)]
    print(f"🧩 Generated {len(ranges)} chunks with step size: {step}")

    # 4. Function to write individual chunk
    def write_chunk(r):
        r_start, r_end = r
        chunk_df = df.filter((F.col(index_column) >= r_start) & (F.col(index_column) <= r_end))
        if add_chunk_id:
            chunk_df = chunk_df.withColumn("chunk_id", F.lit(f"{r_start}_{r_end}"))
        row_count = chunk_df.count()
        print(f"📦 Writing chunk: {r_start} to {r_end} | Rows: {row_count}")
        
        chunk_df.write \
            .mode(save_mode) \
            .format("delta") \
            .option("maxRecordsPerFile", max_records_per_file) \
            .save(output_path)
        
        print(f"✅ Done: {r_start} to {r_end} | Files created with max {max_records_per_file} rows each")

    # 5. Run in parallel or sequentially
    if parallel:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(write_chunk, ranges)
    else:
        for r in ranges:
            write_chunk(r)

    print("🎉 All chunks written to Delta table successfully!")
