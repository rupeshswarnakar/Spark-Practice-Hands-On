# Spark-Practice-Hands-On

## Question 1
```
Customer Data Cleaning
Problem Statement
●	As a data engineer you are given the task to clean the customer data
●	Your pipeline should remove all the duplicates records
●	And also remove those records which are duplicated on the basis of height and age

Data

Smith,23,5.3
Rashmi,27,5.8
Smith,23,5.3
Payal,27,5.8
Megha,27,5.4
Metadata- columns

Name - String
Age - Integer
Height - double
```

## Solution 1
```
data = [("Smith", 23, 5.3),
            ("Rashmi", 27, 5.8),
            ("Smith", 23, 5.3),
            ("Payal", 27, 5.8),
            ("Megha", 27, 5.4
             )]

columns = StructType([ \
StructField("Name", StringType(), True), \
StructField("Age", IntegerType(), True), \
StructField("Height", DoubleType(), True)
])

Df = spark.createDataFrame(data=data, schema=columns)
Df2 = Df.dropDuplicates(["Name", "Age", "Height"])
Df2.show(truncate=False)
```
## Question 2
```
City Temperature Analysis
Problem Statement	
- As a data engineer you are supposed to prepare the data from temp analysis
- Your pipeline should return data in form of following columns
○	city
○	avg_temperature
○	total_temperature
○	num_measurements
- You should return metrics for only those cities when total_temperature is greater than 30
- And output should be sorted on city in ascending order

Data
 New York , 10.0  
 New York , 12.0 
 Los Angeles , 20.0  
 Los Angeles , 22.0 
 San Francisco , 15.0  
 San Francisco , 18.0
Metadata- columns

city - String
temperature - Double

```
## Solution 2
```
data = [("New York", 10.0),
            ("New York", 12.0),
            ("Los Angeles", 20.0),
            ("Los Angeles", 22.0),
            ("San Francisco", 15.0),
            ("San Francisco", 18.0
)]

schema = StructType([ \
StructField("city", StringType(), True), \
StructField("temperature", DoubleType(), True) \
])

df = spark.createDataFrame(data=data, schema=schema)
df2 = df.groupBy("city") \
.agg(avg("temperature").alias("avg_temp"), \
sum("temperature").alias("total_temp"), \
count("temperature").alias("num_measurement")) \
.where(col("total_temp") > 30) \
.orderBy(col("city").asc()

df2.show(truncate=False)
```


## Question 3:
```
Sample data for Department.txt
Finance,10,100
Marketing,20,200
IT,40,300

data schema to be inferred on above data
dept_name string,
dept_id integer,
salary long


Write a pyspark program which reads the above file from hadoop and introduces a new column in the existing dataframe with name as doubleSalary which should have values twice as the existing salary. Convert the data into a parquet file named department.parquet. You can store this converted parquet file either on the local file system or on hadoop.
Create Department.txt in /home/takeo dir
○	nano Department.txt
○	ctrl + s
○	ctrl + x

○	hadoop fs -mkdir -p /data/spark/test
○	hadoop fs -put ~/Department.txt /data/spark/test

schema = StructType() \
.add("dept_name",StringType(),True) \
.add("dept_id",IntegerType(),True) \
.add("salary",LongType(),True)

df_with_schema = spark.read.format("csv").schema(schema).load("/data/spark/test/Department.txt")

df2 = df_with_schema.withColumn("doubleSalary",col("salary")*2)

df2.write.parquet("/data/spark/test/parquet/department.parquet")


Expected data in department.parquet
+---------+------+------------+
|dept_name | salary | doubleSalary|
+---------+------+------------+
|Finance  |  10             |20          |
|Marketing|  20            |40          |
|Sales    |    30            |60          |
|IT          |    40            |80          |
+---------+------+------------+
```

# Question 4: 
```
student.json
{"name":{"firstname":"James","middlename":"","lastname":"Smith"},"languages":["Java","Scala","C++"],"state":"OH","gender":"M"}
{"name":{"firstname":"Anna","middlename":"Rose","lastname":""},"languages":["Spark","Java","C++"],"state":"NY","gender":"F"}
{"name":{"firstname":"Julia","middlename":"","lastname":"Williams"},"languages":["CSharp","VB"],"state":"OH","gender":"F"}
{"name":{"firstname":"Maria","middlename":"Anne","lastname":"Jones"},"languages":["CSharp","VB"],"state":"NY","gender":"M"}
{"name":{"firstname":"Jen","middlename":"Mary","lastname":"Brown"},"languages":["CSharp","VB"],"state":"NY","gender":"M"}
{"name":{"firstname":"Mike","middlename":"Mary","lastname":"Williams"},"languages":["Python","VB"],"state":"OH","gender":"M"}


Write a pyspark program which reads the above json file either from local or hadoop file system and returns the first name and gender of all those students who are learning java and does not belong to state OH. 

df = spark.read.json("file:///home/takeo/student.json")
df.createOrReplaceTempView("JSONTable")
jsonSQL = spark.sql(“select name.firstname AS firstname, gender from JSONTable where array_contains(languages, ‘Java’) AND state != ‘OH’)
jsonSQL.show()


+---------+------+
|firstname|gender|
+---------+------+
|     Anna|     F|
+---------+------+
```



Q3.
employee.json
{"employee_name":"James","department":"Sales","salary":3000}
{"employee_name":"Michael","department":"Sales","salary":4600}
{"employee_name":"Robert","department":"Sales","salary":4100}
{"employee_name":"Maria","department":"Finance","salary":3000}
{"employee_name":"James","department":"Sales","salary":3000}
{"employee_name":"Scott","department":"Finance","salary":3300}
{"employee_name":"Jen","department":"Finance","salary":3900}
{"employee_name":"Jeff","department":"Marketing","salary":3000}
{"employee_name":"Kumar","department":"Marketing","salary":2000}
{"employee_name":"Saif","department":"Sales","salary":4100}



Write a pyspark program which read employee.json and do the followings
1.	Generate an orc file partitioned on a department for distinct employees.
•	df = spark.read.json("file:///home/takeo/employee.json")
•	df_distinct = df.distinct()

•	df_distinct.write.mode("overwrite").partitionBy("department").format("orc").save("file:///home/takeo/orc/department_part”)

2.	Return departments names in descending orders with their mean salary. 

•	df = spark.read.json("file:///home/takeo/employee.json")
•	df.createOrReplaceTempView("employee")

•	jsonSQL = spark.sql(“Select department, AVG(salary) AS avg_salary FROM employee GROUP BY department ORDER BY department DESC”)

•	jsonSQL.show()



Q4.
employee.json
{"emp_id":1,"name":"Smith","superior_emp_id":-1,"year_joined":"2018","emp_dept_id":"10","gender":"M","salary":3000}
{"emp_id":2,"name":"Rose","superior_emp_id":1,"year_joined":"2010","emp_dept_id":"20","gender":"M","salary":4000}
{"emp_id":3,"name":"Williams","superior_emp_id":1,"year_joined":"2010","emp_dept_id":"10","gender":"M","salary":1000}
{"emp_id":4,"name":"Jones","superior_emp_id":2,"year_joined":"2005","emp_dept_id":"10","gender":"F","salary":2000}
{"emp_id":5,"name":"Brown","superior_emp_id":2,"year_joined":"2010","emp_dept_id":"40","gender":"","salary":-1}
{"emp_id":6,"name":"Brown","superior_emp_id":2,"year_joined":"2010","emp_dept_id":"50","gender":"","salary":-1}


department.json
{"dept_name":"Finance","dept_id":10}
{"dept_name":"Marketing","dept_id":20}
{"dept_name":"Sales","dept_id":30}
{"dept_name":"IT","dept_id":40}


Write a pyspark program which collects all the department names, departments max salary and number of employees in each department and persist collected data into a partitioned hive table with parquet as internal file format for hive. And dept_name as a hive table partitioned column. And part_department as hive table name.

•	df_employee = spark.read.json("file:///home/takeo/employee.json")
•	df_department = spark.read.json("file:///home/takeo/department.json")
•	
•	df_employee.createOrReplaceTempView("employee")
•	df_department.createOrReplaceTempView("department")

•	jsonSQL = spark.sql("SELECT d.dept_name, MAX(e.salary) AS max_salary, COUNT(e.emp_dept_id) AS employee_count FROM employee AS e INNER JOIN department AS d ON CAST(e.emp_dept_id AS INT) = d.dept_id GROUP BY d.dept_name")
•	jsonSQL.show()

•	jsonSQL.write.mode("overwrite").partitionBy("dept_name").format("parquet").saveAsTable("default.part_department")


Expected output data in hive table
+---------+---------+--------------+
|dept_name|maxSalary|employeesCount|
+---------+---------+--------------+
|  Finance|     3000|             3|
|Marketing|     4000|             1|
|       IT|       -1|             1|
+---------+---------+--------------+

Q5.
Create a file simple-zipcodes.csv for below data 
RecordNumber,Country,City,Zipcode,State
1,US,PARC PARQUE,704,PR
2,US,PASEO COSTA DEL SUR,704,PR
10,US,BDA SAN LUIS,709,PR
49347,US,HOLT,32564,FL
49348,US,HOMOSASSA,34487,FL
61391,US,CINGULAR WIRELESS,76166,TX
61392,US,FORT WORTH,76177,TX
61393,US,FT WORTH,76177,TX
54356,US,SPRUCE PINE,35585,AL
76511,US,ASH HILL,27007,NC
4,US,URB EUGENE RICE,704,PR
39827,US,MESA,85209,AZ
39828,US,MESA,85210,AZ
49345,US,HILLIARD,32046,FL
49346,US,HOLDER,34445,FL
3,US,SECT LANAUSSE,704,PR
54354,US,SPRING GARDEN,36275,AL
54355,US,SPRINGVILLE,35146,AL
76512,US,ASHEBORO,27203,NC
76513,US,ASHEBORO,27204,NC

•	nano simple-zipcodes.csv
•	ctrl + s
•	ctrl + x
•	df = spark.read.csv(“file:///home/takeo/simple-zipcodes.csv”, header=True, inferschema=True)
•	df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ZipcodeProcessing") \
    .enableHiveSupport() \
    .getOrCreate()



Write a python program for followings
●	Read the above data and generate a new sample data with a sample size of 50 %.
sample_df = df.sample(withReplacement=False, fraction=0.5)
sample_df.show()

●	Create a hive partitioned table on state and city with maximum 3 records in each data file within the partitions.  

sample_df.write.mode(“overwrite”).partitionBy(“State”, “City”).option(“maxRecordsPerFile”=3).format(“parquet”).savAsTable(“default.zipcodes_part”)

●	Run a hive sql in pyspark to get the data for states other than AL and cities other than SPRINGVILLE.

result_df = spark.sql(“SELECT * FROM default.zipcodes_part WHERE State != ‘AL’ AND City != ‘SPRINGVILLE’”)
result_df.show()
<img width="454" height="690" alt="image" src="https://github.com/user-attachments/assets/8a008c13-d2a8-4adb-8044-ff1fc95bb19c" />


