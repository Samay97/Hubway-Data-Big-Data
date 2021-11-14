
import argparse
import ast
from datetime import datetime
from math import atan2, cos, pi, sin, sqrt

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType


def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Optimize Hubway-Data stored within HDFS')  
    parser.add_argument('--filenames', required=True, type=str)
    return parser.parse_args()

def degreesToRadians(degrees):
    """
    Calculate degree to radian
    """
    return degrees * pi / 180

def distanceInKmBetweenEarthCoordinates(lat1, lon1, lat2, lon2):
    """
    Get a disatnce in KM, from two GPS positions
    https://stackoverflow.com/questions/365826/calculate-distance-between-2-gps-coordinates
    """
    try:
        earthRadiusKm = 6371
        dLat = degreesToRadians(lat2-lat1)
        dLon = degreesToRadians(lon2-lon1)
        lat1 = degreesToRadians(lat1)
        lat2 = degreesToRadians(lat2)
        a = sin(dLat/2) * sin(dLat/2) + sin(dLon/2) * sin(dLon/2) * cos(lat1) * cos(lat2)
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return round(earthRadiusKm * c, 3)
    except TypeError:
        # Case no Station is set
        return 0

def get_age(birth_year):
    """
    Calculate the age, in years, based on birth year
    """
    try:
        return datetime.now().year - int(birth_year)
    except ValueError:
        # Case no birth date is set
        return 0


if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Parse all filenames
    filenames = []
    try:
        filenames = ast.literal_eval(args.filenames)
    except:
        folder_name = None
    
    if len(filenames) >= 1:
        
        # optimize each csv
        for filename in filenames:
            folder_name = ''

            try:
                folder_name = filename.split('-')[0]
            except:
                folder_name = None

            if folder_name is not None:

                print('###### Optimizing ######')
                print('CSV in: {}'.format(folder_name))
                print('########################')

                # Read csv to dataframe
                test_csv_dataframe = spark.read.format('csv')\
                    .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
                    .load('/user/hadoop/hubway/raw/{}/hubway-tripdata.csv'.format(folder_name))

                
                # convert to a UDF Function by passing in the function and return type of function
                udf_distanceInKmBetweenEarthCoordinates = F.udf(distanceInKmBetweenEarthCoordinates, FloatType()) 
                udf_get_age = F.udf(get_age, IntegerType())

                # add tripdistance
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'tripdistance',
                    udf_distanceInKmBetweenEarthCoordinates(
                        'start station latitude', 
                        'start station longitude', 
                        'end station latitude', 
                        'end station longitude'
                    )
                )

                # add age
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'age',
                    udf_get_age(
                        'birth year'
                    )
                )

                # convert string date to unix timestamp
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'starttime', 
                    F.unix_timestamp('starttime')
                )

                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'stoptime', 
                    F.unix_timestamp('stoptime')
                )

                # Drop not used colums
                final_df = test_csv_dataframe.drop('start station latitude', 'start station longitude', 'end station latitude', 'end station longitude', 'usertype', 'birth year')

                # Safe back to hdfs
                final_df.coalesce(1)\
                    .write\
                    .format('csv')\
                    .option("header", "true")\
                    .mode('overwrite')\
                    .save('/user/hadoop/hubway/final/{}/hubway-tripdata.csv'.format(folder_name))
