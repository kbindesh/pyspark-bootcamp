# Create your first Spark application using IDE

## Step-01: Create a new Pycharm project

## Step-02: Develop the Spark application

### Step-2.1: Create a Spark Session

- A SparkSession can be used to: 
  1. Create DataFrame
  2. Register DataFrame as tables
  3. Execute SQL over tables
  4. Cache tables
  5. Read parquet files 
- To create a SparkSession, use the following builder pattern:

```

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master("local")
        .appName("Word Count")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)
```
- For more details, refer https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html#pyspark-sql-sparksession

### Step-2.2: Create Data List

```
data_list = [
    ("Bindesh", 29),
    ("Ashish", 33)
]
```
### Step-2.3: Create a DataFrame from the DataList

```
df = spark.createDataFrame(data_list).toDF("Name","Age")
df.show()
```

### Resulting Program
```
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
```