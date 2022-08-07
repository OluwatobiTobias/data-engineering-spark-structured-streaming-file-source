# Learning Data Engineering

## Project Description

This is a Spark streaming program that streams data from a file source(e.g CSV, JSON, Parquet, AVRO), applies aggregations with SparkSQL and sends the data and its aggregations to a sink (e.g console, file source).

## Project Objective

To demonstrate a simple Spark streaming with aggregation using PySpark.

![spark_streaming](files/assets/spark_streaming.png)

>- Used the Python Faker library to produce fake data files (CSV, JSON) which is populated to a source destination at intervals set with the built-in python time module.

>- Define the table schema with StructType.

>- Create a spark session, spark readstream object and a output spark writestream object.

>- Performed aggregation on the data with SparkSQL (i.e with SQL query).

## Setup/How to run

- Inside the cloned repo, create your virtual env, and run **`pip install -r dependencies.txt`** in the activated virtual env to set up required libraries and packages.

- Activte the virtual env.

- It would be a good idea to split the terminal, run **`python spark_streaming_from_csv.py`** then **`python generation_csv_from_faker.py`** in the terminal.

- I would advice not to delay time in running both scripts as the spark streaming session is timed, otherwise change the numerial argument in `.awaitTerminatio()`. Any number gives it a time in secs and nothing awaits for a keyboard or program interruption.

- Any acceptable file format by Spark can be used with this program with a well defined schema. :+1:

## Improving our project

- We can include scheduling and orchestration with Apache Airflow.

Having any issues, reach me at this [email](oluwatobitobias@gmail.com). More is definitely coming, do come back to check them out.

Now go get your cup of coffee/tea and enjoy a good code-read and criticism :+1: :+1:.
