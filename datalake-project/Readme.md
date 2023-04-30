## Summary
The requirment is to build a ETL pipeline for Sparkify analytical team to gather insights on their user base songs listening pattern
<br/>
We will be creating an ETL pipeline using Spark and Datalake to create set of facts and dimensions table. We will also use AWS EMR clusters to deploy our Spark jobs

## Analysis
- Data is available in AWS S3 as JSON file
- We will load the data from S3, into our Spark jobs deployed on AWS EMR
- To develop the ETL pipeline for Spark job, we will use Notebook that can directly interface with AWS EMR cluster

## Project Artifacts
- dwh.cfg
```
   This is the config file that will store all the configuration data to for our ETL job
```
- etl.py
```
  This script will load the file from Songs and Logs data from S3 and create the sets of facts and dimension table.
```
## Usage
```bash
  
  ## Edit the dwh.cfg for the following values
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=

    input_data=
    output_data=

  pip install -r requirements.txt

  ## Run the ETL job to launch the Spark jobs
  python etl.py

  Now the Datalake is ready for analysis
```

## Citation
```
   Getting the latest row for users data
   https://stackoverflow.com/questions/59886143/spark-dataframe-how-to-keep-only-latest-record-for-each-group-based-on-id-and
```