from pyspark.sql.functions import col

sas_description_file = 'I94_SAS_Labels_Descriptions.SAS'
extract_only_city_name = lambda key, value: value.split(',')[0]


def dict_to_tuples(dict):
    return [(k, v) for k, v in dict.items()]


def extract_codes(line_start, line_end):
    """
    Extracts data from the I94_SAS_Labels_Descriptions.SAS file
    :param line_start:
    :param line_end:
    :return:
    """
    codes_start = line_start
    codes_end = line_end
    codes = {}
    with open(sas_description_file, 'r') as file:
        lines = file.readlines();
        start_processing = False
        for line in lines:
            if codes_start in line:
                start_processing = True
            elif codes_end in line:
                start_processing = False;
            elif start_processing:
                key, value = line.strip().split('=')
                key = key.strip().strip(''').strip(''')
                value = value.strip().strip(''').strip(''').strip()
                codes[key.replace('\'', "")] = value.replace('\'', "")
    return codes


def extract_city_codes():
    """
    Extract Cities data from I94_SAS_Labels_Descriptions.SAS
    :return:
    """
    city_codes = extract_codes('$i94prtl', ';')
    for key in city_codes:
        city_codes[key] = extract_only_city_name(key, city_codes[key])
    return city_codes


def extract_country_codes():
    """
    Extracts Country data from I94_SAS_Labels_Descriptions.SAS
    :return:
    """
    country_codes = extract_codes('i94cntyl', ';')
    return country_codes


def extract_state_codes():
    """
    Extracts States data from I94_SAS_Labels_Descriptions.SAS
    :return:
    """
    state_codes = extract_codes('i94addrl', ';')
    return state_codes


def extract_visa_types():
    """
    Extract VisaTypes data from I94_SAS_Labels_Descriptions.SAS
    :return:
    """
    visa_types = extract_codes('I94VISA', '*/')
    return visa_types


def cleanup_weather_data(df_weather):
    """
    Clean up weather data, filter for US and also from year 2010
    :param df_weather:
    :return:
    """
    df_weather = df_weather.filter(
        df_weather.Country == 'United States')
    df_weather = df_weather.filter(
        df_weather.date >= '2010-01-01')
    return df_weather


def group_cities_demographics_data(df_cities_demographics):
    """
    Groups cities demographics data as it was provided for each race
    :param df_cities_demographics:
    :return:
    """
    df_cities_demographics = df_cities_demographics.groupBy('city', 'state_code') \
        .sum("male_population", 'female_population', 'total_population', 'number_of_veterans', 'foreign_born')
    df_cities_demographics = df_cities_demographics.drop('race').drop('count') \
        .withColumnRenamed('sum(male_population)', 'male_population') \
        .withColumnRenamed('sum(female_population)', 'female_population') \
        .withColumnRenamed('sum(total_population)', 'total_population') \
        .withColumnRenamed('sum(number_of_veterans)', 'number_of_veterans') \
        .withColumnRenamed('sum(foreign_born)', 'foreign_born')
    return df_cities_demographics


def check_dataframe_rows(dataframe, raise_exception):
    """
    Util function to check if dataframe is not empty
    :param dataframe:
    :param raise_exception:
    :return:
    """
    dataframe_rows = dataframe.count()
    if dataframe_rows <= 0:
        print(f'Error Dataframe {dataframe} has empty rows')
        if raise_exception:
            raise Exception(f'Error Dataframe {dataframe} has empty rows')
    else:
        print(f'Dataframe {dataframe} has valid rows')


def check_dataframe_fields(dataframe, raise_exception, columns_to_check):
    """
    Util function to check if dataframe schema has the columns
    :param dataframe:
    :param raise_exception:
    :param columns_to_check:
    :return:
    """
    df_columns = dataframe.columns;
    for column in columns_to_check:
        if column in df_columns:
            print(f'Dataframe has column {column}')
        else:
            print(f'Error Dataframe does not have column {column}')
            if raise_exception:
                raise Exception(f'Error Dataframe {dataframe} does not have column {column}')


def check_dataframe_pk(dataframe, raise_exception, column):
    """
    Util function to check if the PK of the dataframe is not null
    :param dataframe:
    :param raise_exception:
    :param column:
    :return:
    """
    count = dataframe.select(col(column).isNull()).count()
    print(f'Count of column {column} null is: {count}')
    if count > 0:
        print(f'Error Dataframe has column {column} which is null ')
        if raise_exception:
            raise Exception(f'Error has column {column} which is null ')
