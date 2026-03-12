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

