# map() vs flatmap()
'''
map(): applied to each row of RDD; output rdd will have same number of rows as input rdd.
flatmap(): applied to each row of RDD;output rdd may have more number or same number of rows as input rdd.
'''
from pyspark import SparkContext, SparkConf

sc = SparkContext("local","map vs flatmap()")

def map_flatmap():
    print("\n\ngenerating RDD from csv data \n\n")
    file_data = sc.textFile("/Users/varshasingh/PycharmProjects/PySpark_practice/Data/short.csv")
    print(file_data.take(5))

    for i in file_data.take(5):
        print(i)

    print(" \n applying map() to split on basis of delimiter & extracting first row from RDD \n ")
    file_data=file_data.map(lambda x:x.split(","))
    print(file_data.take(5))
    file_data=file_data.map(lambda x: x[0])
    print(file_data.take(15))

    print(" \n applying flatmap() to split on basis of delimiter\n ")
    file_data=file_data.flatMap(lambda x: x[0])
    print(file_data.take(5))
    for i in file_data.take(5):
        print(i)

map_flatmap()

'''
rdd2=rdd1.filter(lambda x: x!=10)
rddx = rdd2.map(lambda x: (x,1))
'''