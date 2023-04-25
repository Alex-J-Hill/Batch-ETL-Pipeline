# Batch-ETL-Pipeline

The purpose of this project was to learn to leverage various AWS services to deploy a daily batch ETL pipeline.

## AWS services used
1. S3 for storage
2. CloudWatch for scheduling trigger
3. Lambda for pulling in data and handling success/failure notification duties of the extraction
4. EC2 for development and Airflow hosting
5. EMR for Spark cluster hosing and job processing
6. Glue + Athena for enabling external querying of the processed data

## Non-AWS services
1. Snowflake for mimicking a transactional database by storing the source data and pushing to S3
2. PowerBI for analysis and visualization

## Architecture diagram
![Alt text](https://github.com/Alex-J-Hill/Batch-ETL-Pipeline/blob/main/ArchitectureDiagram.png "Architecture Diagram")

