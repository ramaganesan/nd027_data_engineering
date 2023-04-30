## Summary
Requirements is to create a datawarehousing cluster in AWS Redshift so that business can analyse the songs listened by their user base.
<br/>
We also need to create the ETL pipeline that will load data into the Redshift Datawarehouse

## Analysis
- Data is available in AWS S3 as JSON file
- We will load the data from S3 into a staging table
- From staging tables we will load the fact and dimension tables
- Based on the analysis of data we decided to used user id as dist key in songsplay table
- Since songs data is huge, for the songs and songs staging table, dist key is artist id

## Project Artifacts
- dwh.cfg
```
   This is the config file that will store all the configuration data to create Redshift cluster
```
- redshift_cluster.py
  ```
  This script will create, delete, opentcp access to Redshift cluster
  Usage
  python redshift_cluster.py --option create => Create Redshift cluster based on config in dwh.cfg

  python redshift_cluster.py --option delete => Delete the Redshift cluster

  python redshift_cluster.py --option opentcp => Open TCP connection on Redshift cluster port from outside the cluster VPC

  ```
- sql_queries.py
```
   This script contains all the DDL, DML and Redshift COPY commands of our cluster
```
- create_tables.py
```
   This script contains the code that will create, drop tables
```
- etl.py
```
   This script contains the ETL process that will load data into our Redshift cluster
```

## Usage
```bash
  set in user environment with AWS IAM user account information
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and REGION

  ## Edit the dwh.cfg for the following values
    [AWS]
    REGION=us-west-2

    [DWH] 
    DWH_CLUSTER_TYPE=multi-node
    DWH_NUM_NODES=4
    DWH_NODE_TYPE=dc2.large

    DWH_IAM_ROLE_NAME=**
    DWH_CLUSTER_IDENTIFIER=**
    DWH_DB=**
    DWH_DB_USER=**
    DWH_DB_PASSWORD=**
    DWH_PORT=**

  pip install -r requirements.txt

  ## Create Redshift cluster
  python redshift_cluster.py --option create

  ## Edit the dwh.cfg for the following values after cluster is created
  [CLUSTER]
    HOST=**
    DB_NAME=**
    DB_USER=**
    DB_PASSWORD=**
    DB_PORT=**

  ## Create tables
  python create_tables.py

  ## Run the ETL job to load data from S3
  python etl.py

  Now the Datawarehouse is ready for analysis
```

## Clean up (Important for not incurring costs)
```
  python redshift_cluster.py --option delete
```

## Citation
```
  converting ts in milliseconds to timestamp
   https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
```

