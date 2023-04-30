# Introduction

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. This project will create the necessary database schema and the ETL process to facilitate Sparkify to analyze the data

## Requirements

To run the process we need Logs data and Songs data to be dumped into the data folder. The ETL process will look for files under the data folder

The ETL process is created as a python script so a suitable python working environment is needed

The ETL process will need access to a POSTGRES database, so the schema can be created

## Database Schema
Based on the analysis of the data, the following schema has been designed. 
After analysis of the Songs data from Sparkify we decided to create the SONGS and ARTISTS dimension table. 
After analysis of the Logs data from Sparkify we decided to create USERS, TIME dimension table.
With the analysis of the Logs, we decided to create the SONGPLAYS FACT table, which can store the songs the users are listening to on the Sparkify APP. 
This schema design provides us with the ability to analyze the data and satisfy the requirements to get insights into the songs the user base is listening to
 

```
 DIMENSIONS tables
SONGS
ARTISTS
USERS
TIME

FACTS tables
SONGPLAYS

```

![Database Schema](./sparkifydb_erd.png)

## Usage

```python
!python create_tables.py

!python etl.py
```

## Project Files

```python
create_tables.py => This script creates the Database schema

python etl.py  => This script will process the logs data and Songs data located in /data folder

data folder => This folder holds the data that the ETL job will process for analysis  
```
