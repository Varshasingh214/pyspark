from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Common Col & Distinct Col join").getOrCreate()
print(spark)

schema =["id","name","dept"]
data =[("1","vivek","science"),("2","varsha","commerce"),("3","deepti","arts"),("4","devansh","humanities")]
df1 = spark.createDataFrame(data,schema)
df1.show()

schema2= ["emp_id","sal"]
data2 = [(1,2000),(2,3000),(3,3000),(4,4000)]
df2 = spark.createDataFrame(data2,schema2)
df2.show()

print("--------joining both the df on matching column----------")
df_join = df1.join(df2,'id','inner')
df_join.show()

print("--------joining both the df on non-matching column----------")
df_join1 = df1.unionByName(df2,allowMissingColumns=True)
df_join1.show()