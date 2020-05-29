# PROJECT DESCRIPTION:This project is about creating a data wareshouse sparkifydwhcluster for sparkify music streaming app. 

Below are the files in the project:
**dwh_cfg:** Contains the configurations parameters to connect to S3,  Redshift cluster and the sparkifydb
**sql_queries.py:** Contains the DDLs of the tables, insertion statements, drop statements along with a select query to fetch primary keys from the dimension tables songs, artists for the fact table.
**create_tables.py:** File to execute the ddls, insertions queries in the sql_queries file
**etl.py:** To list, process all the json files and insert data into the tables
**Trigger.ipynb:** To trigger the .py files

The source data (_s3://udacity-dend/song_data_, _s3://udacity-dend/song_data_) from S3 is in JSON format. We have staging tables _staging_events_ and _staging_songs_ in Redshift which are loaded with the raw data from the source JSON files. The json file (_s3://udacity-dend/log_json_path.json_) is used to parse the source file correctly and load data into the staging table _staging_events_.
We have created a Fact table (_SongPlays_) and four dimension tables (_songs, artists, users, time_) which will be loaded with the data from the staging tables in Redshift sparkifydb.