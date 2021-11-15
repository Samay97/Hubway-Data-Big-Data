
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
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)

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
    except ValueError:
        # catch float errors
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


# is_time_between_0 - is_time_between_3 complete dumb
# but it works, to get a work around with the scope and not passing normal var in function is_time_between
def is_time_between_0(begin_time, end_time):    
    check_time_start = begin_time.replace(hour=0, minute=0, second=0)
    check_time_end = end_time.replace(hour=5, minute=59, second=59)
    
    if ((begin_time >= check_time_start and begin_time <= check_time_end) or
        (end_time >= check_time_start and end_time <= check_time_end)):
        return 1
    elif begin_time <= check_time_start and end_time >= check_time_end:
        return 1
    return 0

def is_time_between_1(begin_time, end_time):    
    check_time_start = begin_time.replace(hour=6, minute=0, second=0)
    check_time_end = end_time.replace(hour=11, minute=59, second=59)
    
    if ((begin_time >= check_time_start and begin_time <= check_time_end) or
        (end_time >= check_time_start and end_time <= check_time_end)):
        return 1
    elif begin_time <= check_time_start and end_time >= check_time_end:
        return 1
    return 0

def is_time_between_2(begin_time, end_time):    
    check_time_start = begin_time.replace(hour=12, minute=0, second=0)
    check_time_end = end_time.replace(hour=17, minute=59, second=59)
    
    if ((begin_time >= check_time_start and begin_time <= check_time_end) or
        (end_time >= check_time_start and end_time <= check_time_end)):
        return 1
    elif begin_time <= check_time_start and end_time >= check_time_end:
        return 1
    return 0

def is_time_between_3(begin_time, end_time):    
    check_time_start = begin_time.replace(hour=18, minute=0, second=0)
    check_time_end = end_time.replace(hour=23, minute=59, second=59)
    
    if ((begin_time >= check_time_start and begin_time <= check_time_end) or
        (end_time >= check_time_start and end_time <= check_time_end)):
        return 1
    elif begin_time <= check_time_start and end_time >= check_time_end:
        return 1
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

                # Work around, very bad but it works :D
                udf_is_time_in_timeslot0 = F.udf(is_time_between_0, IntegerType())
                udf_is_time_in_timeslot1 = F.udf(is_time_between_1, IntegerType())
                udf_is_time_in_timeslot2 = F.udf(is_time_between_2, IntegerType())
                udf_is_time_in_timeslot3 = F.udf(is_time_between_3, IntegerType())

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

                # convert starttime and stoptime to timeslots

                # 0-6
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'timeslot_1', 
                    udf_is_time_in_timeslot0(
                        'starttime',
                        'stoptime'
                    )
                )
                # 6-12
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'timeslot_2', 
                    udf_is_time_in_timeslot1(
                        'starttime',
                        'stoptime'
                    )
                )
                # 12-18
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'timeslot_3', 
                    udf_is_time_in_timeslot2(
                        'starttime',
                        'stoptime'
                    )
                )
                # 18-24
                test_csv_dataframe = test_csv_dataframe.withColumn(
                    'timeslot_4', 
                    udf_is_time_in_timeslot3(
                        'starttime',
                        'stoptime'
                    )
                )

                # Drop not used colums
                final_df = test_csv_dataframe.drop(
                    'start station latitude', 
                    'start station longitude', 
                    'end station latitude',
                    'end station longitude',
                    'usertype',
                    'birth year',
                    'starttime',
                    'stoptime'
                )

                # Safe back to hdfs
                final_df.coalesce(1)\
                    .write\
                    .format('csv')\
                    .option("header", "true")\
                    .mode('overwrite')\
                    .save('/user/hadoop/hubway/final/{}/hubway-tripdata.csv'.format(folder_name))
