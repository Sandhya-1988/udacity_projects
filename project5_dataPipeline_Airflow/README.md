
#Purpose of the project:
The purpose of this project to learn how to automate and monitor Data Warehouse ETL Pipelines using Apache Airflow.Using Airflow we can create high-grade data pipelines that allow backfills, monitor them and also ensure data quality.
Also, after the above is done, we need to run some tests to avoid any discrepancy.

#DAG for the Airflow:
1. udac_example_dag.py -> This file consists of the entire DAG for the project. Reference and call to all the necessary tasks.
2. stage_redshift.py -> Operator inserting the data for song and events from S3 bucket into staging tables in Redshift.
3. load_fact.py -> Operator inserting data into fact table songplays from staging tables.
4. load_dimension.py- > Operator inserting data into dimension tables from staging tables.
5. data_quality.py - > Operator ensuring data quality such that there is no discrepancy in data.
6. sql_queries.py -> Insert queries for fact and dimension tables.
7. create_tables.sql -> Queries to create tables in Redshift.

# Steps to run DAG on Airflow:
1. Create a Redshift Cluster and when the status is "Available", create all the tables from "create_tables.sql" using Query Editor.
2. Complete your airflow code.
3. Now in Terminal, write :
                    - /opt/airflow/start.sh
                    - pip install awscli
                    - aws configure -> Enter necessary details 
4. Open Airflow web.
5. Go to Admin Tab and click on "Connections".
6. Create one for "aws_credetials" and another for "redshift".
7. Now, turn on the DAG and in the Graph View it should look like this in thsi link:
  https://udacity-bucket-demo-sandhya.s3.amazonaws.com/Screen+Shot+2019-07-31+at+1.07.49+PM.png

# Check and verify results:
1. select * from users;
https://udacity-bucket-demo-sandhya.s3.amazonaws.com/Screen+Shot+2019-07-31+at+1.14.09+PM.png
2. select * from songplays;
https://udacity-bucket-demo-sandhya.s3.amazonaws.com/Screen+Shot+2019-07-31+at+1.17.27+PM.png







