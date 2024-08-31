# Setup Development Environment for Spark (Windows)

## Step-01: Prerequisites for Spark setup on Windows

- JDK 8/11/17
- Python 3.11
- Hadoop WinUtils
- Spark Binaries
- Environment variables
- Python IDE - Pycharm

## Step-02: JDK - Install and Configure

- Navigate to https://jdk.java.net/java-se-ri/17-MR1 and click **Windows 11 x64 Java Development Kit (sha256)** to initiate the download.
- Unzip the downloaded \*.zip file and place the unzipped folder (e.g. java-17) in "C:\Program Files\Java" folder.
- Now, set the JAVA_HOME environment variable. Launch Command prompt and execute following commands:

```
# Set the JAVA_HOME envionment variable
setx JAVA_HOME = "C:\Program Files\Java\jdk-17"

[After executing the precending cmd, make sure you see a successful msg]

# Verify the JAVA_HOME variable value
echo %JAVA_HOME%

# Add the Java bin path in the Path variable
setx PATH "%PATH%;%JAVA_HOME%\bin"

[After executing the precending cmd, make sure you see a successful msg]
```

- Close the command prompt window >> Restart the command prompt and check if you're able to access java.

```
java --version

[You should see the java version after running the preceding command]
```

## Step-03: Python - Install and Configure

- Navigate to https://www.python.org/downloads/ and scroll vertically to "**Looking for a specific release?**" section.
- Click on **Python 3.11.9** link which will take you to the download page.
- Scroll vertically to the "**Files**" section and click on **Windows installer (64-bit)** to initiate the download.
- Once download is complete, click on the \*.exe to launch the installation.
- Check the checkbox "**Add Python 3.11 to PATH**" and click **Install Now**.

- Now, launch Command prompt and run following command to check the Python version:

```
python --version

[You should see the current installed version of python i.e 3.11.9]
```

## Step-04: Hadoop WinUtils (allow spark on windows)

- Navigate to https://github.com/cdarlint/winutils/.
- Click on **Code** button >> **Download ZIP**.
- Unzip the downloaded **winutils-master.zip** file. You will get the **winutils-master** folder.
- Create a new folder in C: drive called **Spark**.
- Open the unzipped folder i.e. **winutils-master** >> Copy the latest version of hadoop winutils folder i.e. **hadoop-3.3.6** and paste it C:\Spark folder created in the previous step.
- Setup the environment variable for hadoop winutils as follows:

```
# Create HADOOP_HOME environment variable
setx HADOOP_HOME "C:\Spark\hadoop-3.3.6"

# Add the HADOOP_HOME\bin to the PATH variable
setx PATH "%PATH%;%HADOOP_HOME%\bin"
```

## Step-05: Spark Binaries

- Navigate to https://spark.apache.org/downloads.html
  - **Spark release**: 3.4.3 (Apr 18 2024)
  - **Package type**: Pre-built for Apache Hadoop 3.3 and later
  - **Download Spark**: spark-3.4.3-bin-hadoop3.tgz
- Unzip the downloaded **spark-3.4.3-bin-hadoop3.tgz** file. After unzipping, a new folder will be created i.e. **spark-3.4.3-bin-hadoop3**.
- Open the **spark-3.4.3-bin-hadoop3** folder >> copy the sub-folder **spark-3.4.3-bin-hadoop3** and paste it in the **C:\Spark** folder.
- Rename the **spark-3.4.3-bin-hadoop3** folder to **spark-3.4.3**.
- Now, launch the Command prompt >> Set the **SPARK_HOME** environment variable as follows:

```
# Set the SPARK_HOME environment variable
setx SPARK_HOME "C:\Spark\spark-3.4.3"

# Add the SPARK_HOME\bin to the PATH variable
setx PATH "%PATH%;%SPARK_HOME%\bin"

[In case you see a warning message "The being saved is truncated to 1024 characters", update the PATH environment variable from the GUI]

# Update the SPARK_HOME Path using GUI as below:
- Open Environment Variables >> User variables for xxx >> Path >> Edit >> New >>
- Paste the Spark bin directory path (C:\Spark\spark-3.4.3\bin)>> ok
```

- Check if the spark is working on your system by launching Command prompt and run the following command:

```
pyspark

[You should see spark shell getting launched without any errors]
```

## Step-06: Environment Variables

- In case you see any errors after running the preceding command, you have to add two more env variables namely **PYTHONPATH** and **PYSPARK_PYTHON**. Otherwise, skip this step.

```
# Set PYTHONPATH environment variable | Set it as path of Python and Py4j libraries
setx PYTHONPATH "C:\Spark\spark-3.4.3\python;C:\Spark\spark-3.4.3\python\lib\py4j-0.10.9.7-src.zip"

# Check where exactly python is installed on your system
where python

[Copy the location of the python installation from the output of the preceding command; we'll use it in next command]

# Set PYSPARK_PYTHON environment variable
setx PYSPARK_PYTHON "C:\Users\ServerX\AppData\Local\Programs\Python\Python311\python.exe"
```

- Again, test the pyspark shell from the command prompt:

```
pyspark

[This time you shouldn't see the error message]
```

## Step-07: Python IDE - PyCharm

- Navigate to PyCharm official website's download page, https://www.jetbrains.com/pycharm/download/?section=windows
- Scroll down to **PyCharm Community Edition**, and click **Download** button.

## Step-08: Jupyter Notebook - Install and Configure

- Launch command prompt and run the following command:

```
# Install Jupyter
pip install jupyter

# Create a new Jupyter Notebook
jupyter notebook
```
