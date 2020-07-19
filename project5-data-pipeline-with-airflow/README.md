# Data Pipeline with Airflow
This is the fifth project of the Data Engineering Nanodegree from Udacity. In this project I introduced more automation and monitoring to data warehouse ETL pipelines with Airflow. 

## 1. Purpose
A music streaming company, Sparkify, wants to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, allow easy backfills and data quality check. 

## 2. Configuring DAG

## 3. Building Operators
### 1) Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. 
### 2) Fact and Dimension Operators
Utilize the provided SQL helper class to run data transformations with dimension and fact operators. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. 
### 3) Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually. 