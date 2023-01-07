''' This code consists of:
1. map()
2. mapPartitions()
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("about map & mapPartitions").getOrCreate()
print(spark)

'''map(): applies transformation to each row of dataframe
returns same number of output rows as number of input rows'''


def map():
    data = [("Jake", "23", "M"), ("Alan", "40", "M"), ("Berta", "55", "F"), ("Chelsea", "30", "F")]
    rdd = spark.sparkContext.parallelize(data)       # way to create rdd from sparksession's object
    rdd.collect()
    rdd_map = rdd.map(lambda x: (x, 1))
    print(rdd_map.take(2))

map()

''' mapPartitions() apply map transformation over a partition
useful when to perform heavy initialisation once for each partition'''


def mapPartition():
    print(" \n \n --- start of map partitioning --- \n\n ")
    data1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)
    print(data1.collect())
    print(data1.glom().collect())

    def f(iterator): yield sum(iterator)

    print(data1.mapPartitions(f).collect())

mapPartition()
