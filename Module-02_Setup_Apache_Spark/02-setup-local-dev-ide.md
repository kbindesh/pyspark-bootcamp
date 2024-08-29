# Setup Development Environment (Windows)

## Step-01: Prerequisites for Spark setup on Windows

- JDK 8/11/17
- Python 3.11
- Hadoop WinUtils
- Spark Binaries
- Environment variables
- Python IDE - Pycharm

## Step-02: JDK - Install and Configure

- Navigate to https://jdk.java.net/java-se-ri/17-MR1 and click **Windows 11 x64 Java Development Kit (sha256)** to initiate the download.
- Unzip the downloaded *.zip file and place the unzipped folder (e.g. java-17) in "C:\Program Files\Java" folder.
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
- Once download is complete, click on the *.exe to launch the installation.
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


## Step-06: Environment Variables

## Step-07: Python IDE - PyCharm