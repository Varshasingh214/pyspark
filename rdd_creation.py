'''
Ways to create RDD:
    a. parallelize() for in-memory data object
    b. textFile() & wholeTextFiles() for data stored in file
    c. from another rdd
'''

from pyspark import SparkContext, SparkConf

conf = (SparkConf().setAppName("First RDD Application").setMaster("local[8]"))
sc = SparkContext(conf=conf)
print(sc)

def create_rdd():
    # using parallelize() for in-memory data objects
    data = range(1,100)
    rdd1 = sc.parallelize(data)
    print(rdd1.toDebugString())

    # from files
    rdd_textfile = sc.textFile('./Data/*')
    for i in rdd_textfile.take(5):
        print(i)

    rdd_wholeTextfile = sc.wholeTextFiles('./Data/')
    for i in rdd_wholeTextfile.take(2):
        print(i)

    # another rdd
    print("\n creating rdd from another rdd \n")
    rdd_from_rdd = rdd_textfile.take(5)
    print(rdd_from_rdd)

create_rdd()

