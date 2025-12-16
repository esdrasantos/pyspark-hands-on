from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

# Reading csv data into spark

dummy = spark.read.csv(
    "dummy1.csv",
    header=True,
    inferSchema=True
)

print('Dummy (CSV) Schema:')
dummy.printSchema()

print('Dummy Dataframe:')
dummy.show()

print('Calculating how many values of a column are related to other column')
dummy.crosstab('Country', 'Salary').show()

print('Droping duplicates')
dummy.select('Country','FirstName').dropDuplicates().show()

print('Droping and counting nan row indice ~~ starting by 0')
print(dummy.dropna().count())

print('Filling nan value')
dummy.fillna(-1).show()

print('Counting filtered values ~~ Salary > 217')
print(dummy.filter(dummy.Salary > 217).count())

print('Aggregate of Salary (mean) to every Country')
dummy.groupBy('Country')\
    .agg(F.mean('Salary').alias('mean_salary'))\
    .show()

dummy.groupBy('Country').count().show()

# Sampling dataframes ~~ fraction is the percentage of rows to collect for sample

print('Sampling dataframes')
t1 = dummy.sample(withReplacement=False, fraction=0.6, seed=42)
t2 = dummy.sample(withReplacement=False, fraction=0.6, seed=43)
print('Samples count', t1.count(), t2.count())
t1.show()
t2.show()

print('Ordering dataframe')
dummy.orderBy(dummy.Salary.desc()).show(5)

print('Creating new column')
dummy.withColumn('Salary_increase', dummy.Salary*1.2).select('Salary', 'Salary_increase').show(5)
