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
Customer Data Cleaning
Problem Statement
● As a data engineer you are given the task to clean the customer data
● Your pipeline should remove all the duplicates records
● And also remove those records which are duplicated on the basis of height and age

**Data**
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


