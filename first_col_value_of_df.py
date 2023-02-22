
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import first

spark = SparkSession.builder.appName("Find first col value").getOrCreate()
print(spark)

schema = ['name','age','address']
data = [['vivek',30,'Pune'],['varsha',25,'nashik'],['deepti',23,'hyd'],['devansh',12,'patna'],['shivang',2,'Pune']]

df= spark.createDataFrame(data,schema)
print(df.collect())

first_value = df.select(first('address')).collect()[0][0]
print(first_value)
