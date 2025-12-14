from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Spark-MySQL")
    .config(
        "spark.jars",
        "libs/mysql-connector-j-9.5.0/mysql-connector-j-9.5.0.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df_mysql = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        "jdbc:mysql://mysql:3306/demol"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )
    .option("dbtable", "demotable")
    .option("user", "root")
    .option("password", "root")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

print('Data:')
df_mysql.show()

print('Schema:')
df_mysql.printSchema()