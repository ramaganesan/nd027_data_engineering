from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, dense_rank, dayofweek, lower, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StructField, StructType, StringType, DoubleType, IntegerType
from utils import extract_city_codes, extract_state_codes, extract_country_codes, extract_visa_types, dict_to_tuples, \
    cleanup_weather_data, group_cities_demographics_data, check_dataframe_rows, check_dataframe_fields, check_dataframe_pk

# data input
input_data = 'sas_data'
# This can point to the S3 location where the parquet tables be created
output_data = 'data/output'
epoch = datetime(1960, 1, 1)

get_date_sas = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)


@udf
def get_city_code_from_name(city_dict: dict, city_name: str):
    for key, value in city_dict.items():
        if city_name.lower() == value.lower():
            return key


def create_spark_session():
    """
    Creates Spark Session
    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName("ND027_Capstone_Project") \
        .getOrCreate()
    return spark


def create_immigration_df(spark, input_sas_data):
    """
    Created the Immigration Dataframe by reading the SAS formatted I94 data
    :param spark:
    :param input_sas_data:
    :return: df_immigration
    """
    immigration_data = f'{input_sas_data}/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet'
    df_immigration = spark.read.parquet(immigration_data);

    df_immigration = df_immigration.select(['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
                                            'i94mode', 'i94addr', 'depdate', 'i94visa', 'visatype', 'dtaddto',
                                            'biryear', 'i94bir',
                                            'gender', 'admnum'])
    df_immigration = df_immigration.withColumn('arrival_date_ts', get_date_sas('arrdate').cast(TimestampType()))
    df_immigration = df_immigration.withColumn('departure_date_ts', get_date_sas('depdate').cast(TimestampType()))
    df_immigration = df_immigration.filter(
        df_immigration.cicid.isNotNull())

    # Data Quality Check
    check_dataframe_rows(df_immigration, True)
    columns_to_check = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
                                            'i94mode', 'i94addr', 'depdate', 'i94visa', 'visatype', 'dtaddto',
                                            'biryear', 'i94bir',
                                            'gender', 'admnum']
    check_dataframe_fields(df_immigration, True, columns_to_check)

    return df_immigration


def create_immigration_fact_table(df_immigration):
    """
    Creates the Immigration Fact table
    :param df_immigration:
    :return:
    """
    print(f'Creating Immigration Fact Table')
    df_immigration_fact = df_immigration.select(['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
                                                 'i94mode', 'i94addr', 'depdate', 'i94visa', 'visatype', 'dtaddto',
                                                 'arrival_date_ts', 'departure_date_ts'])
    df_immigration_fact.printSchema()

    # Data Quality Check
    check_dataframe_rows(df_immigration_fact, True)

    df_immigration_fact.write.parquet(os.path.join(output_data, 'immigration_fact'), partitionBy=['i94yr', 'i94mon'],
                                      mode='overwrite')

    print(f'Immigration Fact Table created in {output_data}/immigration_fact')


def create_user_dim_table(df_immigration):
    """
    Create the Users Dimension table
    :param df_immigration:
    :return:
    """
    print(f'Creating User Dimension Table')
    df_user_dim = df_immigration.select(['cicid', 'biryear', 'i94bir', 'gender', 'admnum'])
    df_user_dim.printSchema()

    df_user_dim.write.parquet(os.path.join(output_data, 'users_dimension'),
                              mode='overwrite')
    print(f'User Dimension Table created in {output_data}/users_dimension')


def create_country_dim_table(spark):
    """
    Creates the Countries Dimension table
    :param spark:
    :return:
    """
    print(f'Creating Country Dimension Table')
    countries_codes = extract_country_codes()
    countries_codes = dict_to_tuples(countries_codes)
    schema = StructType([
        StructField('country_code', StringType(), True),
        StructField('name', StringType(), True)
    ])

    df_countries_dim = spark.createDataFrame(data=countries_codes, schema=schema)
    df_countries_dim.printSchema()

    # Data Quality Check
    check_dataframe_rows(df_countries_dim, True)

    df_countries_dim.write.parquet(os.path.join(output_data, 'countries_dimension'), mode='overwrite')
    print(f'Country Dimension Table created in {output_data}/countries_dimension')


def create_state_dim_table(spark):
    """
    Create the State Dimension table
    :param spark:
    :return:
    """
    print(f'Creating States Dimension Table')
    state_codes = extract_state_codes()
    state_codes = dict_to_tuples(state_codes)
    schema = StructType([
        StructField('state_code', StringType(), True),
        StructField('name', StringType(), True)
    ])

    df_states_dim = spark.createDataFrame(data=state_codes, schema=schema)
    df_states_dim.printSchema()

    # Data Quality Check
    check_dataframe_rows(df_states_dim, True)

    df_states_dim.write.parquet(os.path.join(output_data, 'states_dimension'), mode='overwrite')
    print(f'States Dimension Table created in {output_data}/states_dimension')


def create_cities_demographics_dim_table(spark):
    """
    Create the Cities Dimension table
    :param spark:
    :return:
    """
    print(f'Creating Cities Demographics Dimension Table')

    schema = StructType([
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('median_age', DoubleType(), True),
        StructField('male_population', IntegerType(), True),
        StructField('female_population', IntegerType(), True),
        StructField('total_population', IntegerType(), True),
        StructField('number_of_veterans', IntegerType(), True),
        StructField('foreign_born', IntegerType(), True),
        StructField('average_household_size', DoubleType(), True),
        StructField('state_code', StringType(), True),
        StructField('race', StringType(), True),
        StructField('count', IntegerType(), True)
    ])
    cities_demographics_data = 'us-cities-demographics.csv'
    df_cities_demographics = spark.read.csv(cities_demographics_data, sep=';', header=False, schema=schema)
    df_cities_demographics = group_cities_demographics_data(df_cities_demographics)

    city_codes = extract_city_codes()
    city_codes = dict_to_tuples(city_codes)

    schema = StructType([
        StructField('city_code', StringType(), True),
        StructField('city', StringType(), True)
    ])
    df_city_codes = spark.createDataFrame(data=city_codes, schema=schema)
    df_city_codes.show()

    df_cities_demographics = df_cities_demographics.join(df_city_codes,
                                                         lower(df_cities_demographics.city) == lower(
                                                             df_city_codes.city), 'left').drop(df_city_codes.city)

    df_cities_demographics.dropDuplicates()
    df_cities_demographics.printSchema()

    df_cities_demographics.filter(
        df_cities_demographics.city_code.isNotNull()).show()

    df_cities_demographics.write.parquet(os.path.join(output_data, 'cities_demographics_dimension'), mode='overwrite')
    print(f'Cities Demographics Dimension created in {output_data}/cities_demographics_dimension')


def create_visatype_dim_table(spark):
    """
    Create the VisaType Dimension table
    :param spark:
    :return:
    """
    print(f'Creating Visatypes Dimension Table')
    visa_types = extract_visa_types()
    visa_types = dict_to_tuples(visa_types)

    schema = StructType([
        StructField('visa_code', StringType(), True),
        StructField('type_desc', StringType(), True)
    ])
    df_states_dim = spark.createDataFrame(data=visa_types, schema=schema)
    df_states_dim.printSchema()

    df_states_dim.write.parquet(os.path.join(output_data, 'visa_types_dimension'), mode='overwrite')
    print(f'Visatypes Dimension Table created in {output_data}/visa_types_dimension')


def create_time_dim_table(df_immigration):
    """
    Create the Time Dimension table
    :param df_immigration:
    :return:
    """
    print(f'Creating Time Dimension Table')
    ##using both arrival and departure dates
    arrival_date_ts = df_immigration.select('arrival_date_ts')
    departure_date_ts = df_immigration.select('departure_date_ts')
    df_time = arrival_date_ts.union(departure_date_ts)
    df_time = df_time.withColumnRenamed('arrival_date_ts', 'time_stamp')
    df_time.dropDuplicates()

    time_table = df_time.select('time_stamp')
    time_table = time_table.withColumn('hour', hour('time_stamp'))
    time_table = time_table.withColumn('day', dayofmonth('time_stamp'))
    time_table = time_table.withColumn('week', weekofyear('time_stamp'))
    time_table = time_table.withColumn('month', month('time_stamp'))
    time_table = time_table.withColumn('year', year('time_stamp'))
    time_table = time_table.withColumn('weekday', dayofweek('time_stamp'))
    time_table.printSchema()

    time_table.write.parquet(os.path.join(output_data, 'time_dimension'), mode='overwrite')
    print(f'Time Dimension Table created in {output_data}/time_dimension')


def create_weather_dim_table(spark):
    """
    Creates the Weather Dimension table
    :param spark:
    :return:
    """
    print(f'Creating Weather Dimension Table')
    schema = StructType([
        StructField('dt', DateType(), True),
        StructField('AverageTemperature', DoubleType(), True),
        StructField('AverageTemperatureUncertainty', DoubleType(), True),
        StructField('City', StringType(), True),
        StructField('Country', StringType(), True),
        StructField('Latitude', IntegerType(), True),
        StructField('Longitude', IntegerType(), True)
    ])
    weather_data = 'GlobalLandTemperaturesByCity.csv'
    df_weather = spark.read.csv(weather_data, header=True, schema=schema)
    df_weather = df_weather.withColumn('date', to_date('dt'))

    # Cleaning up weather data
    df_weather = cleanup_weather_data(df_weather)

    city_codes = extract_city_codes()
    city_codes = dict_to_tuples(city_codes)
    schema = StructType([
        StructField('city_code', StringType(), True),
        StructField('city_name', StringType(), True)
    ])
    df_city_codes = spark.createDataFrame(data=city_codes, schema=schema)

    # Only creating weather data for cities in our fact table
    df_weather = df_weather.join(df_city_codes,
                                 lower(df_weather['City']) == lower(
                                     df_city_codes['city_name']))
    df_weather = df_weather.drop(df_weather.city_name)

    df_weather.printSchema();
    print(df_weather.tail(20))

    df_weather.write.parquet(os.path.join(output_data, 'weather_dimension'), mode='overwrite')
    print(f'Weather Dimension Table created in {output_data}/weather_dimension')


def main():
    """
    Main Function that runs our ETL process
    :return:
    """
    spark = create_spark_session()

    df_immigration = create_immigration_df(spark, input_data)

    create_immigration_fact_table(df_immigration)

    create_user_dim_table(df_immigration)

    create_time_dim_table(df_immigration)

    create_cities_demographics_dim_table(spark)

    create_country_dim_table(spark)

    create_state_dim_table(spark)

    create_visatype_dim_table(spark)

    create_weather_dim_table(spark)

    spark.stop()


if __name__ == "__main__":
    main()
