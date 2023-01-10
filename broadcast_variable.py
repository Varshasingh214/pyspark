# Use of broadcast variables
'''
We cannot broadcast a variable directly to rdd, dataframe as the data inside them is scattered.
create a dictionary for variable that has to be broadcast.
Using map(),apply this broadcasted variable on an index
'''
import pyspark
from pyspark.sql.session import SparkSession

def broadcast_rdd():
    spark = SparkSession.builder.appName("About broadcast").getOrCreate()
    print(spark)

    dept = {"CSE": "Computer Science", "ISE": "Information Sci", "ECE": "Electronic & Comm", "TCE": "Telecom"}
    print("--------------- broadcasting dept ----------------------")
    broadcast_dept = spark.sparkContext.broadcast(dept)

    emp_data = [("Harry","1","CSE"),("James","2","ISE"),("Megha","3","ECE"),("Prince","4","TCE")]
    rdd = spark.sparkContext.parallelize(emp_data)
    # print(rdd.take(4))

    def broadcast_fun(full_dept):
        return broadcast_dept.value[full_dept]

    output = rdd.map(lambda x: (x[0],x[1],broadcast_fun(x[2]))).collect()
    print(output)

# broadcast_rdd()

def broadcast_df():
    spark = SparkSession.builder.appName("About broadcast").getOrCreate()
    print(spark)

    dept = {"CSE": "Computer Science", "ISE": "Information Sci", "ECE": "Electronic & Comm", "TCE": "Telecom"}
    print("--------------- broadcasting dept ----------------------")
    broadcast_dept = spark.sparkContext.broadcast(dept)

    emp_columns = ["NAME","ID","BRANCH"]
    emp_data = [("Harry", "1", "CSE"), ("James", "2", "ISE"), ("Megha", "3", "ECE"), ("Prince", "4", "TCE")]
    emp_df = spark.createDataFrame(emp_data).toDF(*emp_columns)
    print("creating df .....")
    emp_df.show()

    def broadcast_fun(full_dept):
        return broadcast_dept.value[full_dept]

    print("updating branch .......")
    output = emp_df.rdd.map(lambda x: (x[0], x[1], broadcast_fun(x[2]))).toDF(emp_columns)
    print(output)
    output.show()

broadcast_df()



