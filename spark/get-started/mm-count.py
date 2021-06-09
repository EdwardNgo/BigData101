import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: mmcout <file>",file=sys.stderr)
        sys.exit(-1)
    spark = SparkSession.builder.appName("PythonMMCount").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    mm_file = sys.argv[1]
    mm_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(mm_file)
    count_mm_df = mm_df.select("State","Color","Count")\
                .groupBy("State","Color")\
                .agg(count("Count").alias("Total"))\
                .orderBy("Total",ascending = False)
    count_mm_df.show(n = 60,truncate = False)
    print("Total row = %d" % count_mm_df.count())


    # While the above code aggregated and counted for all
    # the states, what if we just want to see the data for
    # a single state, e.g., CA?
    # 1. Select from all rows in the DataFrame
    # 2. Filter only CA state
    # 3. groupBy() State and Color as we did above
    # 4. Aggregate the counts for each color
    # 5. orderBy() in descending order
    # Find the aggregate count for California by filtering
    ca_count_mm_df = (mm_df
    .select("State", "Color", "Count")
    .where(mm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))
    # Show the resulting aggregation for California.
    # As above, show() is an action that will trigger the execution of the
    # entire computation.
    ca_count_mm_df.show(n=10, truncate=False)
    spark.stop()