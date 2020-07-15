# ELT Pipeline in Data Lake 
This is the fourth project of the Data Engineering Nanodegree from Udacity. In this project, I appled Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I loaded data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. I deployed this Spark process on a cluster using AWS.

## 1. Purpose
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 

## 2. Schema
### 1) Fact table
`songplays` - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### 2) Dimension tables
`users` - user_id, first_name, last_name, gender, level\
`songs` - song_id, title, artist_id, year, duration\
`artists` - artist_id, name, location, latitude, longitude\
`time` - start_time, hour, day, week, month, year, weekday

## 3. ELT Pipeline
### 1) Processed song_data
Loaded `song_data` in json format from S3, then extracted needed fields to 2 dimensional tables: `songs` and `artists`. Finally wrote above tables to parquet files and stores back to S3
### 2) Processed log_data
Loaded `log_data` in json format from S3, then extracted needed fields to 3 dimensional tables: `users`, `time` and `songplays`. Finally wrote above tables to parquet files and stores back to S3

## 4. Files in repository
### 1) `etl.py`
Executes the ELT pipeline in Python codes, which reads data from S3, processes that data using Spark, and writes them back to S3
### 2) `dl.cfg`
Contains the AWS credentials
### 3) `README.md`
Discuss the purpose of this database in context of the startup, Sparkify and also state the database schema design and ETL pipeline.

## 5. How to run the Python scripts
Make sure the AWS credentials are correct and stored in `dl.cfg` file, submit `etl.py` to EMR cluster. In order to track task process, there will be messages printed out.  