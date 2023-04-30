## Summary
In this project we provided Star schema model to analyse US Immigration data. By providing the Star schema model, users can anlyze the data as per the requirements.  
Creating this data model would enable us to answer the below questions  
- Can get insights into the top countries immigrants came from 
- US city, state that took most immigrants
- Top visa categories whether it was Business Visa, Visit visa etc
- If the immigrant came here on visit visa, did they choose a warm or cold place
- Do we get more visitors during summer months or winter months


## Scope
In this project first we analysed the data. Based on the data that was provided and also the analysis 
- We decided to use PySPark to set up the ETL Pipeline. Spark provides greater flexibility than Hadoop because it can work on a local machine as well as EMR clusters
- The output of the ETL pipeline will be Apache Parquet format which can later be injected into RedShift for further analysis. Generating Parquet files provides us the flexibility running the code local, as well as scale up when needed. We could easily load the Parquet files in Redshift clusters if needed.
- Star schema model is chosen for the reasons below
  - Querying for data can be done using simple joins
  - Aggregation operations can be much faster
  - We can easily build OLAP cubes on top of the Star schema model

Improvements:
- Create EMR clusters to create the ETL pipeline
- Output the Parquet files to AWS S3 for better storage and flexibility
- We can build OLAP cubes on top of the model

## Datasets:
- Immigration data is coming from the SAS Parquet files
- Weather data imported into the project as csv file GlobalLandTemperaturesByCity.csv
- Cities, VisaTypes, State, Countries Dimensions table data is sourced from I94_SAS_Labels_Descriptions.SAS
- Other Dimension table data is sourced from the Immigration data Parquet files
- ** Note, Weather data is not uploaded into Git to download the data go to
    https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data, download the file `GlobalLandTemperaturesByCity.csv` and place the file in the main folder before running the ETL process
## Star Schema
![Schema Diagram](capstone_start_schema.png)

## Data Model
### Immigration Fact Table
|    Column Name    |                                   Description                                   |
|-------------------|---------------------------------------------------------------------------------|
|       cicid       |                                   Primary Key                                   |
|       i94yr       |                                  4 digit year                                   |
|      I94MON       |                                  Numeric month                                  |
|      I94CIT       |  3 digit birth country code of Immigrant. FK to the countries_dimension table   |
|      I94RES       | 3 digit resident country code of Immigrant. FK to the countries_dimension table |
|      i94port      |    Immigrant City of Arrival. FK to the cities_demographics_dimension table     |
|      arrdate      |    Immigrant Arrival Date in SAS format. Will be converted into normal date     |
|      i94mode      |         Immigrant Mode of Arrival. 1=Air, 2=Sea, 3=Land, 9=Not Reported         |
|      i94addr      |          Immigrant State of Arrival. FK to the states_dimension table           |
|      depdate      |                     Immigrant Departure date in SAS format.                     |
|      i94visa      |             Immigrant Visa codes 1=Business, 2=Pleasure, 3=Student              |
|     visatype      |                          Immigrant Class of admission                           |
|      dtaddto      |                          Immigrant I94 expiration date                          |
|  arrival_date_ts  |                     Timestamp of date derived from arrdate                      |
| departure_date_ts |                     Timestamp of date derived from depdate                      |

### Users Dimension table
| Column Name |        Description         |
|-------------|----------------------------|
|    cicid    |        Primary Key         |
|   biryear   |   4 Immigrant birth year   |
|   i94bir    | Age of Respondent in Years |
|   gender    |      Immigrant Gender      |
|   admnum    |    I94 Admission Number    |

### Countries Dimension table
| Column Name  | Description  |
|--------------|--------------|
| country_code | Primary Key  |
|     name     | Country Name | 

### State Dimension table
| Column Name | Description |
|-------------|-------------|
| state_code  | Primary Key |
|    name     | State Name  |  

### Cities Demographics Dimension table
|    Column Name     |               Description               |
|--------------------|-----------------------------------------|
|     city_code      |               Primary Key               |
|        city        |                City Name                |
|     state_code     | State Code. FK to State Dimension table |
|  male_population   |          Total Male population          |
| female_population  |         Total Female population         |
|  total_population  |            Total population             |
| number_of_veterans |           Veterans population           |
|    foreign_born    |         Foreign born population         |

### VisaType Dimension table
| Column Name |   Description    |
|-------------|------------------|
|  visa_code  |   Primary Key    |
|  type_desc  | Visa Description |  

### Time Dimension table
| Column Name | Description |
|-------------|-------------|
| time_stamp  | Primary Key, derived from arrival date and departure date |
|    hour     |    hour     |
|     day     |     day     |
|    week     |    week     |
|    month    |    month    |
|    year     |    year     |
|   weekday   |   weekday   |

### Weather Dimension table
|          Column Name          |                          Description                          |
|-------------------------------|---------------------------------------------------------------|
|              dt               |                         Weather Date                          |
|      AverageTemperature       |                      AverageTemperature                       |
| AverageTemperatureUncertainty |                 AverageTemperatureUncertainty                 |
|             City              |                             City                              |
|            Country            |                            Country                            |
|           Latitude            |                           Latitude                            |
|           Longitude           |                           Longitude                           |
|             date              |              date, with city_code treated as PK               |
|           city_code           | city_code, with date treated a PK. FK to City Dimension table |

## ETL Process
- Create the Spark Session
- Load the Immigration SAS Data using the Apache Parquet files as a Dataframe
- Use the Parquet Dataframe and construct our Immigration Fact table
- Use the Parquet Dataframe and create the Time, Users Dimension tables
- Parse the I94_SAS_Labels_Descriptions.SAS file and create the Countries, State, Cities Dimension tables
- Parse the GlobalLandTemperaturesByCity.csv to create the Weather Dimension table
- During the process do data quality checks as well 

## Project File
```
etl.py => Main Spark ETL process
utils.py => Contains utils function used by the ETL process
data_lake_analysis.ipynb => Jupyter Notebook that provides sample analysis of ETL output

```

## Data Clean up
```
immigration_dataframe_load => This function will check if the data loaded from the source  
is valid and this will abort the process if the loaded data is less than threshold limit.  
This function will help us catch improper load of Immigration Fact Table

check_dataframe_rows => This function checks if the loaded data frame is not empty

check_dataframe_fields => This function checks if the dataframe columns and schema matches

check_dataframe_pk => This function check is the dataframe PK is not null

duplicates_removal => This function removed duplicate rows from dataframe

group_cities_demographics_data => Demographics data is given based on Race, this function  
groups the population data and drops the race column
```

## Running the Process
```
  pip install requirements.txt
  
  python3 etl.py
```

## Other Scenarios
#### The data was increased by 100x:
If the data size increased by 100x then we can create a Spark EMR cluster in AWS to injest the data.  
We can also injest the data into AWS Redshift Cluster to do further analysis

#### The pipelines would be run on a daily basis by 7 am every day:
We can crate an Apache Airflow process to run the ETL on schedule

#### The database needed to be accessed by 100+ people:
The best option for this is to injest the data into AWS Redshift Cluster, so that we can configure users to analyze the data

#### How often data needs to be updated:
Data can be updated on a monthly basis. We partition the immigration data based on year and month, so we can run the analysis for specific month and analyse the results



