PURPOSE OF THE PROJECT:
This project is to meet the requirement of a company called 'SPARKIFY'. They have some data in respect to users and songs and with this data, they want to know the user behavior.Basically, what kind of songs users listen to. So, to extract such information, they have given us files for songs and user behavior.
We have 2 files here: song_data and log_data. 



DATABASE SCHEMA and ETL PIPELINE:
Now, we have the files mentioned above, with the help of creating tables and running queries on them, we can achive ou goal.
So, we create a STAR SCHEMA here, with 
'songplays_table' as FACT table and 
'user_table','time_table','song_table' and 'artist_table' as DIMENSION tables, each having a relation with the fact table.
DATABASE NAME-'sparkifydb' and user as 'student'.
We will CREATE all the above tables using data files, INSERT data into them and will perform SELECT , to crosscheck if data was inserted properly.

sql_queries.py-> write all the sql queries for CRUD operations here.
Execute create_tables.py to create db and run queries.
run etl.ipynb for 1 record and etl.py for all the records.
test your data by executing test.ipynb and running the select queries.

EXAMPLE QUERIES & RESULTS:

sql SELECT * FROM songplays_table LIMIT 5; 

start_time	userid	level	song_id	artist_id	sessionid	location	useragent
1542845032796	15	paid	None	None	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"

1542845350796	15	paid	None	None	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"

1542845526796	15	paid	None	None	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"

1542845741796	15	paid	None	None	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"

1542846220796	15	paid	None	None	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"
