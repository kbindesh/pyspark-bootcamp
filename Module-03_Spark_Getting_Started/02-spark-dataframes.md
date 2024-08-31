# Spark Dataframes

## What will you learn in this module?

- How to load data in Spark?
- How to Query data in Spark?
- Using Spark SQL and DDL statements
- Using Spark DataFrame and DataFrame Reader API
- Working with csv files
- DataFrame transformations and actions
- How to refer Spark API Documentation?

## Step-01: Demystifying Spark Dataframes

- There are basically two approaches to load and query data in Spark:
  1. Using Spark SQL
  2. Using Spark DataFrame API

### 3.1 Understanding Spark Table

### 3.2 Understanding Spark DataFrame

### 3.3 Spark Table vs Spark DataFrame

## Step-02: Create a Spark Dataframe

- Navigate to your Databricks Account and create a new cluster.
  - Name: bin-db-cluster
  - Runtime: <select_the_latest_stable_version>
  - Availability zone: auto
- As cluster is created, from the left-side panel, click **Create** >> **Notebook**.
  - Notebook Name: spark-dataframe-nb
  - Default language: Python
  - Cluster: <select_the_cluster>

### Step-2.1: Create a Spark DataFrame

```
# Create a DataFrame from a json object
file_url = "json_file_path"
df = spark.read.json(file_url)

# Create a DataFrame from a csv object
file_url = "json_file_path"
df = spark.read.csv(file_url)
```

### Step-2.2 Register a Temporary view (SQL API)

```
df.createOrReplaceTempView("filesView")
```