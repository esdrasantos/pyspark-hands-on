from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add


def f_add(spark, v):
    nums = spark.sparkContext.parallelize(v)

    # reduce() is aggregate by function
    adding = nums.reduce(add)
    return adding
    
def f_join(spark, v1, v2):
    x = spark.sparkContext.parallelize(v1)
    y = spark.sparkContext.parallelize(v2)
    joined = v1.join(v2)
    return joined


def main():

    sc = SparkSession.builder.appName("PySparkCreateRDDExample")\
        .config("spark.some.config.option", "some_value")\
        .getOrCreate()

    # Adding
    v1 = [1,2,3,4,5]
    adding = f_add(sc,v1)
    print('Adding all the elements -> %i' %(adding))

    # Joining
    v2 = [("spark",1), ("hadoop",4)]
    v3 = [("spark",2), ("hadoop",5)]
    joined = f_join(sc, v2, v3)
    final = joined.collect()
    print("Joind RDD -> %s" %(final))

    # Caching and cash persistance
    v4 = [['Scala','Java','hadoop','spark','akka','spark vs hadoop','pyspark', 'pyspark and spark']]
    words = sc.sparkContext.parallelize(v4)
    words.cache()
    caching = words.persist().is_cached
    print("Words got cached -> %s" %(caching))

main()

