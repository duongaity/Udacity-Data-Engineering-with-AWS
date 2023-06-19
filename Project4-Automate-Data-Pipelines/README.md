# Data Pipelines

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Overview

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

![Redshift Data Model](./screenshots/airflow_data_model.png)

## Datasets

For this project, you'll be working with two datasets. Here are the s3 links for each:
- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

### Copy S3 Data

```
aws s3 cp s3://udacity-dend/log-data/ ./dataset/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ./dataset/song-data/ --recursive
```

## Project Template

The project template package contains three major components for the project:
- The `dag` template has all the imports and task templates in place, but the task dependencies have not been set
- The `operators` folder with operator templates
- A `helper` class for the SQL transformations

## Configure

### Create an IAM User in AWS

In the Set permissions section, select Attach existing policies directly. Search and select the following policies:
- AdministratorAccess
- AmazonRedshiftFullAccess
- AmazonS3FullAccess

### Configure AWS Redshift Serverless

Grant Redshift access to S3 so it can copy data from CSV files

Create a Redshift Role called my-redshift-service-role from the AWS Cloudshell:

```
aws iam create-role --role-name my-redshift-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'
```

Now give the role S3 Full Access:

```
aws iam attach-role-policy --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess --role-name my-redshift-service-role
```

### Add Airflow Connections to AWS Redshift

On the Airflow create connection page, enter the following values:
- Connection Id: Enter redshift.
- Connection Type: Choose Amazon Redshift.
- Host: Enter the endpoint of your Redshift Serverless workgroup, excluding the port and schema name at the end. You can find this by selecting your workgroup in the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port and schema name at the end of the Redshift endpoint string.
- Schema: Enter dev. This is the Redshift database you want to connect to.
- Login: Enter awsuser.
- Password: Enter the password you created when launching Redshift serverless.
- Port: Enter 5439. Once you've entered these values, select Save.
