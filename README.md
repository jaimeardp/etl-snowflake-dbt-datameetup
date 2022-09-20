Overview
========

Welcome to Astronomer! This project was generated after you ran 'astrocloud dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Using DBT for build models on Snowflake

![Alt text](./images/etl_airflow_snowflake.png?raw=true "Nifi architecture")


Project Contents
================

- dags: This folder contains the Python files for your Airflow DAGs. 
- Dockerfile: This file contains a versioned Astronomer Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

- dags/dbt_config: This file contains settings project needed of DBT for to make ETL 

- load_data/components.snowql : This file contains sql queries for to create external tables using stage snoflake

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astrocloud dev start'.

2. Verify that all 3 Docker containers were created by running 'docker ps'.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.


NIFI Flow data to ingest data to s3
============================================

![Alt text](./images/nifi_snow_to_s3.png?raw=true "Nifi architecture")

