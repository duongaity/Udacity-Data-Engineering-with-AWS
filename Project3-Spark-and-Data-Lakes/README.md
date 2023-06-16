# Spark and Human Balance

## Introduction

In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

## AWS Environment

You'll use the data from the STEDI Step Trainer and mobile app to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:
- Python and Spark
- AWS Glue
- AWS Athena
- AWS S3

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. You can download the data from here or you can extract it from their respective public S3 bucket locations:

### 1. Customer Records (from fulfillment and the STEDI website):

AWS S3 Bucket URI - s3://cd0030bucket/customers/

contains the following fields:
- serialnumber
- birthday
- registrationdate
- customername
- email
- phone
- lastupdatedate
- sharewithpublicasofdate
- sharewithfriendsasofdate
- sharewithresearchasofdate

### 2. Step Trainer Records (data from the motion sensor):

AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

contains the following fields:
- sensorReadingTime
- serialNumber
- distanceFromObject

### 3. Accelerometer Records (from the mobile app):

AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

contains the following fields:
- timeStamp
- user
- x
- y
- z
