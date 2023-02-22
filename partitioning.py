'''
This code will have hands-on example of:
1. repartition()
2. coalesce()
3. partitionBy()
4. exercise to count records present in per partition
'''

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name, spark_partition_id, col

# sc = SparkContext("local[*]","About Partition")
spark = SparkSession.builder.appName("Partitioning in Spark").getOrCreate()
print(spark)

df = spark.read.format("csv").option("header",True).option("header", True).option("inferSchema",True).load("/Users/varshasingh/PycharmProjects/pySpark_learning/Data/*.csv")
df.show(5)

# caching the df will break the lineage, hence it wont be pointing to source thus we cannot see any data in file_name column
# df.cache().count()

print("------printing original number of partitions----------\n")
print(df.rdd.getNumPartitions())
print("------printing number of records per partition----------\n")
new_df = df.withColumn("File_Name",input_file_name()).withColumn("Partition_id",spark_partition_id())
new_df.show(5)
new_df.groupby("Partition_id", "File_Name").count().show(4, False)


# df=df.repartition(5)
# print(df.rdd.getNumPartitions())
# print("-----writing df back to a csv file with repartitioning 5 ")
# df.repartition(5).write.mode('overwrite').csv('../TestOutputData')

# df2 =  df.coalesce(3)
# print("-----writing df back to a csv file with coalesce 5 ")
# df2.coalesce(3).write.mode('overwrite').csv("../Output_Data")
# print(df2.rdd.getNumPartitions())


#partitionBy()
# df3 = spark.read.option("header",True).csv("/Users/varshasingh/PycharmProjects/pySpark_learning/Data/small_data.csv")
# df3.show(5)
# df3.write.option("header",True) \
#         .partitionBy("Variable_name") \
#         .mode("overwrite") \
#         .csv("../Output_Data")


import time
time.sleep(100)
