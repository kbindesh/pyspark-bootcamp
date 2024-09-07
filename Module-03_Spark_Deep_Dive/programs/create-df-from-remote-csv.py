from pyspark.sql import *
from pyspark import SparkFiles

url = "https://raw.githubusercontent.com/kbindesh/sample-datasets/main/covid-19-data-sets/country_wise_latest.csv"

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("HelloSpark") \
            .master("local[2]") \
            .getOrCreate()

    spark.sparkContext.addFile(url)
    df = spark.read.csv(SparkFiles.get("country_wise_latest.csv"), header=True, inferSchema=True)

df.show()