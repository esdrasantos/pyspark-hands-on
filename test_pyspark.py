from pyspark.sql import SparkSession

def increment(x):
    x = x+1
    return x

def print_element(x):
    print(x)

def spark_session_hc_data(spark):

    tp1 = (1,2,3,'abc')
    tp2 = (4,5,6,'def')
    tp3 = (7,8,9,'ghi')


    df = spark.sparkContext.parallelize([tp1, tp2, tp3])\
    .toDF(['col1', 'col2', 'col3', 'col4'])

    df.show()

def spark_session_dummy_data1(spark):

    df_dummy_data = spark.read.format('com.databricks.spark.csv')\
    .options(header='true', inferschema='true')\
    .load('dummy1.csv')

    df_dummy_data.show(5)

    df_dummy_data.printSchema()

    print('Counting elements')
    count = df_dummy_data.count()
    print(count)

    return df_dummy_data


def main():

    spark = SparkSession.builder.appName("PythonSparkCreateRDDExample")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

    # Remember: it's the same as SQL queries!! (select, distinct, so on)

    print("Returning all values of a column")
    df = spark_session_dummy_data1(spark)
    df_sd_column_ft_name = df.select('FirstName')
    df_sd_column_ft_name.foreach(print_element)

    print('Returning distinct values')
    df_sd_column_country = df.select('Country').distinct()
    
    # Foreach is used to apply a function to each element without returning a result
    # If need to return or perform a transformation, use map instead
    df_sd_column_country.foreach(print_element)

    # Filter words or characters inside column values
    words_rdd = df_sd_column_country.rdd.map(lambda row: row[0])
    words_filter = words_rdd.filter(lambda x: 'Queen' in x)
    filtered = words_filter.collect()
    print('Filtered RDD -> %s' %(filtered))

    # Mapping values inside column
    words_map = df_sd_column_country.rdd.map(lambda x: (x,1))
    mapping = words_map.collect()
    print('Key value pair -> %s' %(mapping))


main()