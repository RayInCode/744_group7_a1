# page_rank.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, collect_list, size, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("task4_recovery")
            .config("spark.driver.memory", "30g")  # Sets the Spark driver memory to 30GB
            .config("spark.executor.memory", "30g")  # Sets the Spark executor memory to 30GB
            .config("spark.executor.cores", "5")  # Sets the number of cores for each executor to 5
            .config("spark.task.cpus", "1")  # Sets the number of cpus per task to be 1
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/mnt/data/spark-event-logs")
            .config("spark.local.dir", "/mnt/data/temp") 
            .config("spark.sql.shuffle.partitions", "100")
            .master("spark://10.10.1.1:7077")  
            .getOrCreate())
    # Define schema
    schema = StructType([
        StructField("page", StringType(), True),
        StructField("link", StringType(), True)
    ])
    
    # Load data
    df = spark.read.csv("hdfs://10.10.1.1:9000/data/enwiki-pages-articles", sep="\t", schema=schema)
    
    # Initialize page ranks
    pages = df.select("page").distinct()
    links = df.groupBy("page").agg(collect_list("link").alias("links"))
    ranks = pages.select("page", lit(1).alias("rank"))
    
    # Calculate PageRank
    for iteration in range(4):
        contributions = links.join(ranks, "page").select("links", (col("rank") / size("links")).alias("contribution"))
        contributions = contributions.withColumn("link", explode("links")).select("link", "contribution")
        
        ranks = contributions.groupBy("link").agg(_sum("contribution").alias("sum_contributions"))
        ranks = ranks.select(col("link").alias("page"), (lit(0.15) + lit(0.85) * col("sum_contributions")).alias("rank"))
    
    # Sort the ranks in descending order
    ranks = ranks.orderBy(col("rank").desc())
    
    # Save results to HDFS
    ranks.write.format("csv").mode("overwrite").save("hdfs://10.10.1.1:9000/data/task4_res")
    
    spark.stop()
