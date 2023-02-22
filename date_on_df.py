
#date_format() – function formats Date to String format.
#to_date() – function is used to format string (StringType) to date (DateType) column.


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,date_format

spark = SparkSession.builder.appName(" Working with dates").getOrCreate()
print(spark)

schema = ['subject','date_col','centre']
data = [['english','2020-02-10','kv'],['maths','2020-02-11','sympkins'],['sst','2020-02-13','milton'],['hindi','2020-02-15','aps']]
df = spark.createDataFrame(data, schema)
df.show()

'''
we have a column 'date_col' in a dataframe of format yyyy-MM-dd
convert the format to MM-yyyy-dd
'''

new_df = df.withColumn('new_date',date_format(to_date('date_col','yyyy-MM-dd'),'MM-yyyy-dd'))
new_df.show()

