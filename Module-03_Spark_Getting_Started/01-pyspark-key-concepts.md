# PySpark - Key Concepts

## 01. What is SparkContext?

- A **SparkContext** is low-level component that's the traditional entry point for Spark functionality. 
- It is used for cluster management and creating Resilient Distributed Datasets (RDDs), which are the core data structures of Spark. 
- SparkContext is still supported for backward compatibility, but SparkSession is now the recommended API for structured data processing.

## 02. What is SparkSession?

- A **SparkSession** is a higher-level component 
- Primary entry point for Spark applications. 
- It's used for DataFrame and SQL operations, and provides a more streamlined and user-friendly interface. 
- SparkSession simplifies the code and reduces boilerplate by allowing you to create DataFrames and Datasets directly from various sources.
- Allows programming with DataFrame and DataSet APIs.
- Represents as _spark_ and auto-initialized in a notebook (Zeppelin or Jupyter) type environment.

## 03. What are DataFrames?

- Distributed collection of data organized into named columns.
- Conceptually equivalent to a table in relational database or DataFrame in Python.
- API available in Scala, Java, Python and R.
- 