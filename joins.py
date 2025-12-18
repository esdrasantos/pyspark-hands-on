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

valuesA = [('Pirate',1),('Monkey',2),('Ninga',3),('Spaghetti',4)]
TableA = spark.createDataFrame(valuesA, ['name', 'id'])

valuesB = [('Rutabaga',1), ('Pirate',2), ('Ninja',3), ('Darth Veder', 4)]
TableB = spark.createDataFrame(valuesB, ['name','id'])

TableA.show()
TableB.show()

# Alias atributes a short name to the tables
# Alias here works as 'AS' in SQL.
ta = TableA.alias('ta')
tb = TableB.alias('tb')

print('Inner Join A^B')
inner_join = ta.join(tb, ta.name == tb.name)
inner_join.show()

print('Left Join AvA^B')
left_join = ta.join(tb, ta.name == tb.name, how='left')
left_join.show()

print('Right Join A^BvB')
right_join = ta.join(tb, ta.name==tb.name, how='right')
right_join.show()

print('Full Join AvB')
full_join = ta.join(tb, ta.name == tb.name, how='full')
full_join.show()

print('Left Exclusive Join')
left_join = ta.join(tb, ta.name == tb.name, how='leftsemi')
left_join.show()

print('Cross Join')
crossjoin = ta.crossJoin(tb)
crossjoin.show()
