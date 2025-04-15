from pyspark.sql import DataFrame
from pyspark.sql.functions import floor, rand, col
import math

def apply_salt_and_repartition(
    df: DataFrame,
    skewed_col: str,
    target_partitions: int = None,
    salt_col_name: str = "salt"
) -> DataFrame:
    """
    Applies salting to mitigate skew and repartitions the DataFrame.
    
    Parameters:
    ----------
    df : DataFrame
        The input DataFrame (already loaded from JDBC).
    skewed_col : str
        The column which is skewed (e.g., primary key or join key).
    target_partitions : int, optional
        How many partitions you want post-salting (defaults to current Spark setting).
    salt_col_name : str
        Name of the salt column. Default is "salt".
    
    Returns:
    -------
    DataFrame
        A new DataFrame with a salt column and repartitioned.
    """

    # Estimate skew level: ratio of max count to average
    key_counts = df.groupBy(skewed_col).count()
    stats = key_counts.selectExpr(
        "percentile_approx(count, 0.5) as median_count",
        "max(count) as max_count"
    ).collect()[0]

    median_count = stats["median_count"]
    max_count = stats["max_count"]
    skew_ratio = max_count / median_count if median_count > 0 else 1

    # Define salt factor dynamically based on skew ratio
    # salt_factor = min(math.ceil(skew_ratio), 100)  # Cap at 100 to avoid over-salting
        # Define salt factor dynamically based on skew ratio
    # salt_factor = min(int(math.ceil(skew_ratio)), 100)  # Cap at 100 to avoid over-salting

    if skew_ratio > 50:
        salt_factor = 100
    elif skew_ratio > 20:
        salt_factor = 50
    elif skew_ratio > 10:
        salt_factor = 20
    else:
        salt_factor = 10



    print(f"[INFO] Skew ratio: {skew_ratio:.2f}, using salt factor: {salt_factor}")

    # Add salt column
    df_salted = df.withColumn(salt_col_name, floor(rand() * salt_factor).cast("int"))

    # Define number of partitions if not provided
    if not target_partitions:
        target_partitions = df.sparkSession.conf.get("spark.sql.shuffle.partitions")
        target_partitions = int(target_partitions)

    # Repartition based on skewed_col + salt_col
    repartitioned_df = df_salted.repartition(target_partitions, col(skewed_col), col(salt_col_name))

    return repartitioned_df
