

PROJECT: Data Warehouse

- This project handles data of a music streaming startup, Sparkify. The Sparkify app stores JSON logs for user activity and JSON metadata for the songs in the application. 
These JSON data filed are stored reside in S3 bucket.
The objective of the project is to build an ETL pipeline that extracts the data from S3, stage the data in redshift, and then transform it into dimension tables in redshift.
The ultimate goal is to use the analyticla tables for BI analysis.


- Database schema design:
The analytics database schema is a star design. 
Start design is a simpler design that has  one fact table  and four supporting dimension tables.
Which a understandable design for OLTP porposes.  
ETL pipeline:
Amazon Web Services Redshift is used in ETL pipeline. the pipeline goes as follows:
Extract the original data stored in S# and load it into staging tables in Redshift. And then it load 
the data into a star schema analytical tables in Redshift as well. 
  
 Files:
 create_tables.py (to create the DB to AWS Redshift)
 etl.py (to process all the input data to the DB)
 sql_queries.p (contains all needed queries to build the ETL)