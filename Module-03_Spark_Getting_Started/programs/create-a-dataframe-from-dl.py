from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("HelloSpark") \
            .master("local[2]") \
            .getOrCreate()

    data_list = [
        ("Bindesh", 29),
        ("Ashish", 33)
    ]

    df = spark.createDataFrame(data_list).toDF("Name","Age")
    df.show()