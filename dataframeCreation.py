import pyspark
from pyspark.sql import *

def create_dataframe():
    spark = SparkSession.builder.appName("DF Creation").getOrCreate()
    print(spark)

    print("spark application name:",spark.sparkContext.appName)
    print("spark application id is:",spark.sparkContext.applicationId)

    # 1: dataframe from existing rdd
    # 1st step would be to create a rdd and then apply toDF() to it
    columns = ["language", "count"]
    data = [("eng",30),("hindi",34),("french",67),("germany",43)]
    rdd= spark.sparkContext.parallelize(data)
    # print(rdd.take(2))
    dfFromRDD = rdd.toDF(columns)
    dfFromRDD.printSchema()
    print("\n ---- printing dataframe created using an rdd ---- \n")
    dfFromRDD.show()

    #2: using createDataFrame()
    dffFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)        #chain with toDF() to print column names
    print("\n printing dataframe created using createDataframe API \n")
    dffFromRDD2.show()                                            # to show dataframe in its tabular format

    #3: from python objects
    # from list
    columns1 = ["name","degree","location"]
    data1= [["varsha","ise","blr"],["deepti","mme","hyd"],["anju","bsc","bhr"],["sunil","pg","bhr"]]
    dffromRdd3 = spark.createDataFrame(data1,columns1)
    print("\n printing dataframe created using a list \n")
    dffromRdd3.show()

    #from dictionary
    data2 = [{'name':'rohit','age':'23','occupation':'teacher'},
             {'name':'mohit', 'age':'19', 'occupation':'student'},
             {'name':'sohit','age':'25','occupation':'engineer'}]
    dffromRdd4 = spark.createDataFrame(data2)
    print("\n ---- printing dataframe created using a dictionary ---- \n")
    dffromRdd4.show()

    #from tuple
    data3 = [('calgary','2000'),('alberta','1400'),('kitchener','1800')]
    columns2 = ["city","distance"]
    dffromRdd5 = spark.createDataFrame(data3).toDF(*columns2)
    print("\n ---- printing dataframe created using a tuple ---- \n")
    dffromRdd5.show()

    # from datasources- csv, json, avro, parquet
    # Spark's default file type is parquet
    df_csv = spark.read.option("header", True).option("inferSchema",True).format("csv").load("/Users/varshasingh/PycharmProjects/PySpark_practice/Data/pumpprices.csv")
    print("\n ---- printing dataframe created from a csv file ---- \n")
    df_csv.show(4)
    df_csv.select('Toronto').show()


    '''
    For Spark 2.0+, we write df into csv file as below
    Spark writes data in partitions, so a target folder with partitions will be created.
    using "overwrite" tends to overwrite the previous content of file
    '''
    df_csv.write.mode("overwrite").format("csv").save("Newcsv")

create_dataframe()



