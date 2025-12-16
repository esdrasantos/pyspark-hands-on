from pyspark.sql import SparkSession
from pyspark.sql import Row
import pandas as pd 

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

print('Spark Version:', spark.sparkContext.version)

# Reading dataframe from demol:demotable

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

print('Creating new schema for People')
sc = spark.sparkContext
people_list = [('Karol',27), ('Maria', 8)]
people = [Row(name=name, age=int(age)) for name, age in people_list]
schema_people = spark.createDataFrame(people)

print(type(schema_people))

'''
my_list = [['a',1,2], ['b',2,3], ['c',3,4]]
col_names = ['A', 'B', 'C']
df2 = pd.DataFrame(my_list, columns=col_names)

'''

# Ways to read csv in Spark 2+ , without com.databricks:spark-csv

dummy = spark.read.csv(
    "dummy1.csv",
    header=True,
    inferSchema=True
)

print('Dummy (CSV) Schema:')
dummy.printSchema()

print('Dummy Dataframe:')
dummy.show()

print('Display o Dummy head row (1st one)')
print(dummy.head())

print('Number of rows in dataframe:',dummy.count())

print('Number of columns of dataframe:', len(dummy.columns), '\n')

print('***** Descriptive Statistic of Dataframe (Dummy) *****')
dummy.describe().show()

print('Statistic just for indicated columns:')
dummy.describe('Salary').show()

print('Showing top 5 rows of selected columns:')
dummy.select('FirstName','Salary').show(5)

print('Distinct count of a column values')
print(dummy.select('Salary').distinct().count())

print('Diff values between columns in dataframes')

dummy2 = spark.read.csv(
    "dummy2.csv",
    header=True,
    inferSchema=True
)
dummy.show()
dummy2.show()

diff_cat_in_dummy2_dummy = dummy2.select('Salary')\
                           .subtract(dummy.select('Salary'))

diff_cat_in_dummy2_dummy.show()

print('Number of different values in Salary on 2016 and 2017')
print(diff_cat_in_dummy2_dummy.distinct().count())