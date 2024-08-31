from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("HelloSpark") \
            .master("local[2]") \
            .getOrCreate()

myRange = spark.range(1000).toDF("number")
myRange.show()

# Transformation
divisBy2 = myRange.where("number % 2 = 0")
divisBy2.show()

# Actions
count_d = divisBy2.count()
print(count_d)

# Read a csv file from local system
