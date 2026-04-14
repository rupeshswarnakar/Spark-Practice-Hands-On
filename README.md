# Spark-Practice-Hands-On

## PySpark functions and their use:
1. Create Spark session
```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Interview Practice") \
    .getOrCreate()
```
2. Sample DataFrame
```
from pyspark.sql import Row

data = [
    Row(id=1, name="Alice", dept="HR", salary=5000, age=25),
    Row(id=2, name="Bob", dept="IT", salary=7000, age=30),
    Row(id=3, name="Charlie", dept="IT", salary=8000, age=35),
    Row(id=4, name="David", dept="Finance", salary=6000, age=28),
    Row(id=5, name="Eva", dept="HR", salary=5500, age=26),
]

df = spark.createDataFrame(data)
df.show()
```
3. select()
```
df.select("name", "salary").show()
```
4. filter() / where()
```
df.filter(df.salary > 6000).show()

df.where(df.dept == "IT").show()
```
5. withColumn()
```
from pyspark.sql.functions import col

df.withColumn("bonus", col("salary") * 0.10).show()
```
6. drop()
```
df.drop("age").show()
```
7. distinct()
```
df.select("dept").distinct().show()
```
8. orderBy() / sort()
```
df.orderBy("salary").show()

df.orderBy(col("salary").desc()).show()
```
9. groupBy()
```
from pyspark.sql.functions import sum as spark_sum, avg, max as spark_max

df.groupBy("dept").agg(
    spark_sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    spark_max("salary").alias("max_salary")
).show()
```
10. count()
```
df.groupBy("dept").count().show()
```
11. join()
```
dept_data = [
    Row(dept="HR", manager="John"),
    Row(dept="IT", manager="Sara"),
    Row(dept="Finance", manager="Mike")
]

dept_df = spark.createDataFrame(dept_data)

df.join(dept_df, on="dept", how="inner").show()
```
12. union()
```
new_data = [
    Row(id=6, name="Frank", dept="IT", salary=7500, age=29)
]

df2 = spark.createDataFrame(new_data)

df.union(df2).show()
```

13. dropDuplicates()
```
df.dropDuplicates(["dept"]).show()
```

14. alias()
```
emp = df.alias("emp")
dept = dept_df.alias("dept")

emp.join(dept, col("emp.dept") == col("dept.dept"), "inner").show()
```
15. when() / otherwise()
```
from pyspark.sql.functions import when

df.withColumn(
    "salary_level",
    when(col("salary") >= 7000, "High")
    .when(col("salary") >= 5500, "Medium")
    .otherwise("Low")
).show()
```
16. lit()
```
from pyspark.sql.functions import lit

df.withColumn("country", lit("USA")).show()
```
17. cast()
```
df.withColumn("salary_str", col("salary").cast("string")).show()
```
18. String functions
```
from pyspark.sql.functions import upper, lower, concat, length, trim

df.withColumn("name_upper", upper(col("name"))).show()
df.withColumn("name_lower", lower(col("name"))).show()
df.withColumn("full_info", concat(col("name"), lit("_"), col("dept"))).show()
df.withColumn("name_len", length(col("name"))).show()
df.withColumn("clean_name", trim(col("name"))).show()
```

19. Null handling
```
df.dropna().show()
df.fillna({"dept": "Unknown", "salary": 0}).show()
```

20. Window functions
```
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

df.withColumn("row_num", row_number().over(window_spec)).show()
df.withColumn("rank", rank().over(window_spec)).show()
df.withColumn("dense_rank", dense_rank().over(window_spec)).show()
```

21. collect_list() and collect_set()
```
from pyspark.sql.functions import collect_list, collect_set

df.groupBy("dept").agg(
    collect_list("name").alias("employee_list"),
    collect_set("name").alias("employee_set")
).show(truncate=False)
```

22. explode()
```
from pyspark.sql.functions import explode

array_data = [
    Row(id=1, skills=["Python", "SQL"]),
    Row(id=2, skills=["Spark", "AWS"])
]

array_df = spark.createDataFrame(array_data)

array_df.withColumn("skill", explode(col("skills"))).show()
```
23. split()
```
from pyspark.sql.functions import split

text_data = [
    Row(id=1, tags="python,sql,spark"),
    Row(id=2, tags="aws,hadoop,hive")
]

text_df = spark.createDataFrame(text_data)

text_df.withColumn("tag_array", split(col("tags"), ",")).show(truncate=False)
```

24. explode(split())
```
text_df.withColumn("tag", explode(split(col("tags"), ","))).show()
```
25. regexp_replace()
```
from pyspark.sql.functions import regexp_replace

dirty_data = [
    Row(id=1, phone="(123)-456-7890")
]

dirty_df = spark.createDataFrame(dirty_data)

dirty_df.withColumn(
    "clean_phone",
    regexp_replace(col("phone"), "[^0-9]", "")
).show()
```
26. Date functions
```
from pyspark.sql.functions import current_date, datediff, to_date, year, month

date_data = [
    Row(id=1, join_date="2024-01-15"),
    Row(id=2, join_date="2023-08-20")
]

date_df = spark.createDataFrame(date_data)

date_df = date_df.withColumn("join_date", to_date(col("join_date")))

date_df.withColumn("today", current_date()) \
       .withColumn("days_diff", datediff(current_date(), col("join_date"))) \
       .withColumn("join_year", year(col("join_date"))) \
       .withColumn("join_month", month(col("join_date"))) \
       .show()
```

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
nano Department.txt
ctrl + s
ctrl + x

hadoop fs -mkdir -p /data/spark/test
hadoop fs -put ~/Department.txt /data/spark/test

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

## Question 4: 
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
## Question 5:
```
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



1. Write a pyspark program which read employee.json and do the followings
Generate an orc file partitioned on a department for distinct employees.
df = spark.read.json("file:///home/takeo/employee.json")
df_distinct = df.distinct()
df_distinct.write.mode("overwrite").partitionBy("department").format("orc").save("file:///home/takeo/orc/department_part”)

2. Return departments names in descending orders with their mean salary. 
df = spark.read.json("file:///home/takeo/employee.json")
df.createOrReplaceTempView("employee")

jsonSQL = spark.sql(“Select department, AVG(salary) AS avg_salary FROM employee GROUP BY department ORDER BY department DESC”)
jsonSQL.show()
```


## Question 6:
```
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

df_employee = spark.read.json("file:///home/takeo/employee.json")
df_department = spark.read.json("file:///home/takeo/department.json")

df_employee.createOrReplaceTempView("employee")
df_department.createOrReplaceTempView("department")

jsonSQL = spark.sql("SELECT d.dept_name, MAX(e.salary) AS max_salary, COUNT(e.emp_dept_id) AS employee_count FROM employee AS e INNER JOIN department AS d ON CAST(e.emp_dept_id AS INT) = d.dept_id GROUP BY d.dept_name")
jsonSQL.show()

jsonSQL.write.mode("overwrite").partitionBy("dept_name").format("parquet").saveAsTable("default.part_department")


Expected output data in hive table
+---------+---------+--------------+
|dept_name|maxSalary|employeesCount|
+---------+---------+--------------+
|  Finance|     3000|             3|
|Marketing|     4000|             1|
|       IT|       -1|             1|
+---------+---------+--------------+
```
## Question 7:
```
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

nano simple-zipcodes.csv
ctrl + s
ctrl + x

df = spark.read.csv(“file:///home/takeo/simple-zipcodes.csv”, header=True, inferschema=True)
df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ZipcodeProcessing") \
    .enableHiveSupport() \
    .getOrCreate()

1. Read the above data and generate a new sample data with a sample size of 50 %.
sample_df = df.sample(withReplacement=False, fraction=0.5)
sample_df.show()

2. Create a hive partitioned table on state and city with maximum 3 records in each data file within the partitions.  

sample_df.write.mode(“overwrite”).partitionBy(“State”, “City”).option(“maxRecordsPerFile”=3).format(“parquet”).savAsTable(“default.zipcodes_part”)

3. Run a hive sql in pyspark to get the data for states other than AL and cities other than SPRINGVILLE.

result_df = spark.sql(“SELECT * FROM default.zipcodes_part WHERE State != ‘AL’ AND City != ‘SPRINGVILLE’”)
result_df.show()
```
## Question 8:
```
Q. Create pyspark_sales_etl.py?

A general pipeline idea:
  1. Create raw sales data
  2. Clean and transform columns
  3. Add revenue column
  4. Categorize order size
  5. Aggregate by region and category
  6. Save clean and summary outputs as parquet

Data & Schema:
data = [
    (1, "2026-04-01", "East", "Laptop", "Electronics", 2, 1200.0),
    (2, "2026-04-01", "West", "Phone", "Electronics", 5, 800.0),
    (3, "2026-04-02", "East", "Shoes", "Fashion", 10, 120.0),
    (4, "2026-04-02", "South", "Watch", "Fashion", 3, 250.0),
    (5, "2026-04-03", "West", "TV", "Electronics", 1, 1500.0),
    (6, "2026-04-03", "North", "Jacket", "Fashion", 4, 200.0),
    (7, "2026-04-04", "East", "Tablet", "Electronics", None, 600.0),
    (8, "2026-04-04", "South", "Bag", "Fashion", 6, None),
]

columns = [
    "order_id",
    "order_date",
    "region",
    "product",
    "category",
    "quantity",
    "unit_price"
]
```

## Solution 8:
```
# Import libraries:
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, when, sum as spark_sum, avg, count, round as spark_round)

spark = SparkSession.builder.appName("Sales_ETL_Mini_Project").getOrCreate()

raw_df = spark.createDataFrame(data, columns)
raw_df.show()

# Step 1: Clean data
clean_df = (
    raw_df
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .fillna({"quantity": 0, "unit_price": 0.0})
)

# Step 2: Add derived columns
clean_df = (
    clean_df
    .withColumn("revenue", col("quantity") * col("unit_price"))
    .withColumn(
        "order_size",
        when(col("quantity") >= 8, "Large")
        .when(col("quantity") >= 3, "Medium")
        .otherwise("Small")
    )
)

print("Cleaned and Transformed Data:")
clean_df.show()

# Step 3: Aggregation by region and category
summary_df = (
    clean_df
    .groupBy("region", "category")
    .agg(
        count("order_id").alias("num_orders"),
        spark_sum("quantity").alias("total_quantity"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue_per_order")
    )
    .orderBy("region", "category")
)

print("Summary by Region and Category:")
summary_df.show()

# Step 4: Write outputs
clean_df.write.mode("overwrite").parquet("output/clean_sales")
summary_df.write.mode("overwrite").parquet("output/sales_summary")

spark.stop()

Key Learning:
• uses fillna
• uses to_date
• adds business logic with when
• does grouped aggregations
• writes multiple parquet outputs
```
## Question 9:
```
Q. Create Sales Data Pipeline for below Data and Schema?
data = [
    ("2024-01-01", "CA", "Laptop", 2, 1000),
    ("2024-01-01", "CA", "Phone", 5, 500),
    ("2024-01-02", "NY", "Laptop", 1, 1000),
    ("2024-01-02", "NY", "Phone", 2, 500),
    ("2024-01-03", "CA", "Laptop", 3, 1000),
    ("2024-01-03", "TX", "Tablet", 4, 300),
    ("2024-01-04", "TX", "Laptop", None, 1000),
]

columns = ["date", "state", "product", "quantity", "price"]

**Tasks:**
a. Clean the data
  - Remove or handle NULL quantity
  - Convert date → proper date type

b. Create new column
  - revenue = quantity * price

c. Aggregation tasks
   - Total revenue per state
   - Top selling product per state
   - Daily total revenue
   - Top 2 highest revenue days

d. Create Gold Layer
   - dim_state
   - fact_sales

e. Write to parquet file format
```
## Solution 9:
```
# Creating dataframe
df = spark.createDataFrame(data, columns)
df.show()

a. Clean data
clean_df = (df.fillna({"quantity":0}).withColumn("date", todate(col("date"), "yyyy-MM-dd")))

clean_df.printSchema()
clean_df.show()

b. Create revenue column
sale_df = clean_df.withColumn("revenue", col("quantity") * col("price"))
sale_df.show()

c. Aggregation
# Total revenue per state 
revenue_per_state = sale_df.groupBy("state").agg(spark_sum("revenue").alias("Total_revenue")).orderBy("state")

revenue_per_state.show()

# Top selling product per state
product_state_revenue = sale_df.groupBy("state", "product").agg(spark_sum("revenue").alias("product_revenue")

window_spec = Window.partitionBy("state").orderBy(desc("product_revenue"))

top_product_per_state = 
	product_state_revenue
	.withColumn("rank", row_number().over(window_spec))
	.filter(col("rank") == 1)
	.drop("rank")
	.orderBy("state")

top_product_per_state.show()

# Daily total revenue
daily_total_revenue = sale_df.groupBy("date").agg(spark_sum("revenue").alias("total_daily_revenue")).orderBy("date")

daily_total_revenue.show()

# Top 2 highest revenue days
top_2_days = daily_total_revenue.orderBy(desc("total_daily_revenue")).limit(2)

top_2_days.show()

d. Create Gold Layer
# Create dim_state
dim_state = sale_df.select("state").dropDuplicates().orderBy("state")

# Create fact_sales
fact_sales = sale_df.select("date", "state", "product", "quantity", "price", "revenue")

e. Write to parquet file format
revenue_per_state.write.mode("overwrite").parquet("output/revenue_per_state")
product_state_revenue.write.mode("overwrite").parquet("output/product_state_revenue")
top_product_per_state.write.mode("overwrite").parquet("output/top_product_per_state")
dim_state.write.mode("overwrite").parquet("output/dim_state")
fact_sales.write.mode("overwrite").parquet("output/fact_sales")
```
## Question 10:
### Real-Time Fraud Detection (PySpark Streaming)
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count
import uuid

1. Spark session
spark = SparkSession.builder \
    .appName("Fraud Detection Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

2. Read streaming transaction data
df = spark.readStream \
    .option("header", True) \
    .schema("user_id STRING, amount DOUBLE, timestamp TIMESTAMP") \
    .csv("data/transactions/")

3. Rule 1: High-value transaction (fraud)
high_value = df.filter(col("amount") > 10000)

4. Rule 2: Too many transactions in short time
txn_count = df.groupBy(
    col("user_id"),
    window(col("timestamp"), "1 minute")
).agg(count("*").alias("txn_count"))

frequent_txn = txn_count.filter(col("txn_count") > 5)

5. Write fraud alerts
def write_alerts(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    
    batch_df.write \
        .mode("append") \
        .json("output/fraud_alerts/" + str(uuid.uuid4()))

# 6. Start streaming queries
query1 = high_value.writeStream \
    .foreachBatch(write_alerts) \
    .option("checkpointLocation", "checkpoint/high_value/") \
    .start()

query2 = frequent_txn.writeStream \
    .foreachBatch(write_alerts) \
    .option("checkpointLocation", "checkpoint/frequent_txn/") \
    .start()

spark.streams.awaitAnyTermination()

Key Learnings
- Window functions in streaming
- Real-time aggregations
- Event-time processing
- Streaming pipelines
```
