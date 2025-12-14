from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

spark = (
    SparkSession.builder
    .config("spark.jars", "libs/mysql-connector-j-9.5.0.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("OFF")

print()
print('SPARK VERSION', spark.version)
print('JDBC DRIVER', spark._jvm.com.mysql.cj.jdbc.Driver)


java_import(spark._jvm, "com.mysql.cj.jdbc.Driver")
print("MYSQL DRIVER OK")