# Purpose of the database for sparkify in respect to their analytical goals
The purpose of this project is to move the Data Warehouse for Sparkify to a Data Lake.Like in previus cases ,their data resides in S3 bucket.So, for this we need to build an ETL pipeline that extracts their data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables.After this the data can be used for analytics to find out what songs are users listening to. We can test the database by running multiple analytical queries.

Movement from Data Warehouse to Data Lake is required due to tfollowing reasons:
1. DWH supports only tabular format whereas DL supports al formats.
2. We need to know the Schema before ingeston in DWH whereas in DL since its Schema on Read, so its on the fly scenario.
3. DWH users are mainly Business Analysts whereas, DL users can be Data Scientists, Business Analysts and Machine Learning Engineers as well.

# DB Schema design and ETL Pipeline
1. Data available
   - Song data: s3://udacity-dend/song_data
   - Log data: s3://udacity-dend/log_data
2. Schema Design(in form of parquet files)
   - songplays_table = Data is inserted here partitioned by Year and Month.
   - Artist_Table - This table contains the termiartists data like name,location,latitude and longitude.
   - User_Table - This table contains user data like first name,last name , gender and level.
   - Song_Table - This table contains song data like title,year, duration,etc. Data is partitioned by Artist_id and Year.
   - Time_Table - This table contains the time related data.
3. ETL Pipeline
   - Extract the json data from S3 udacity buckets 
   - Process the data in Spark, Schema on Read as per the tables structures.
   - Load the same into S3 bucket in form of parquet files for further analysis and usage.

S3 Bucket path - https://s3.console.aws.amazon.com/s3/buckets/udacity-bucket-demo-sandhya/tables/?region=us-west-2&tab=overview


# Example queries and their results
For testing purpose and for my own learning I have created tables in AWS ATHENA using AWS Glue Crawler and below is an example of the same.

SELECT * FROM songplays_table;
!(IMAGE OF RESULTS OF SONGPLAYS)
https://udacity-bucket-demo-sandhya.s3.amazonaws.com/Screen+Shot+2019-07-13+at+11.41.35+PM.png
