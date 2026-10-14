from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("WikiClickstream")
         .config("spark.driver.memory", "4g").getOrCreate())
df = spark.read.csv("clickstream-enwiki-2026-09.tsv.gz", sep="\t",
                    schema="prev STRING, curr STRING, type STRING, n LONG")
df.show(5, truncate=False)
print("Total rows:", df.count())
df.write.mode("overwrite").parquet("clickstream_raw_parquet")
print("Parquet conversion completed!")
