import argparse
import ast

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import FloatType, IntegerType


def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Optimize Hubway-Data stored within HDFS')  
    parser.add_argument('--filenames', required=True, type=str)
    return parser.parse_args()

def arr_to_str(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'


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

        # Create new empty df with col

        columns = [
            'year', 'month', 'avg_trip_duration', 'avg_trip_distance', 
            'gender_share', 'age_share', 'top_used_bikes', 
            'top_start_stations', 'top_end_stations', 'time_slots'
        ]
        
        rows = []

        # calc kpi´s for each dataset
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
                dataframe = spark.read.format('csv')\
                    .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
                    .load('/user/hadoop/hubway/final/{}/hubway-tripdata.csv'.format(folder_name))

                year = folder_name[:4]
                month = folder_name[4:]
                
                avg_trip_duration = dataframe.select(F.avg('tripduration').alias('avg')).collect()[0]['avg']
                avg_trip_duration = round(avg_trip_duration / 60, 0) # /60 to get min

                avg_trip_distance = dataframe.select(F.avg('tripdistance').alias('avg')).collect()[0]['avg']
                avg_trip_distance = round(avg_trip_distance, 3)
                
                gender_share_m = dataframe.where(F.col('gender') == 1).select(F.count('gender').alias('count')).collect()[0]['count']
                gender_share_f = dataframe.where(F.col('gender') == 2).select(F.count('gender').alias('count')).collect()[0]['count']
                gender_share_d = dataframe.select(F.count('gender').alias('count')).collect()[0]['count']
                m = gender_share_m / gender_share_d * 100
                w = gender_share_f / gender_share_d * 100
                d = (gender_share_d-gender_share_m-gender_share_f) / gender_share_d * 100
                # m, w, d
                gender_share = [round(m, 2), round(w, 2), round(d, 2)]
                gender_share = arr_to_str(gender_share)

                # tuple(age, count)
                age_share = []
                for i in dataframe.groupBy('age').count().orderBy(F.desc('count')).collect():
                    age_share.append(tuple(i))
                age_share = arr_to_str(age_share)

                # tuple(bikeid, count)
                top_used_bikes = []
                for i in dataframe.groupBy('bikeid').count().orderBy(F.desc('count')).limit(10).collect():
                    top_used_bikes.append(tuple(i))
                top_used_bikes = arr_to_str(top_used_bikes)

                # tuple(start station id, count)
                top_start_stations = []
                for i in dataframe.groupBy('start station id').count().orderBy(F.desc('count')).limit(10).collect():
                    id, count = i
                    name = dataframe.where(F.col('start station id') == id).select('start station name').limit(1).collect()[0]['start station name']
                    top_start_stations.append(tuple((name, count)))
                top_start_stations = arr_to_str(top_start_stations)

                
                # tuple('end station id, count)
                top_end_stations = []
                for i in dataframe.groupBy('end station id').count().orderBy(F.desc('count')).limit(10).collect():
                    id, count = i
                    name = dataframe.where(F.col('end station id') == id).select('end station name').limit(1).collect()[0]['end station name']
                    top_end_stations.append(tuple((name, count)))
                top_end_stations = arr_to_str(top_end_stations)


                # tuple(timeslot, percentage)
                time_slots = []
                time_slot_1 = dataframe.where(F.col('timeslot_1') == 1).groupBy('timeslot_1').count().collect()[0]['count']
                time_slot_2 = dataframe.where(F.col('timeslot_2') == 1).groupBy('timeslot_2').count().collect()[0]['count']
                time_slot_3 = dataframe.where(F.col('timeslot_3') == 1).groupBy('timeslot_3').count().collect()[0]['count']
                time_slot_4 = dataframe.where(F.col('timeslot_4') == 1).groupBy('timeslot_4').count().collect()[0]['count']
                slot_count = time_slot_1 + time_slot_2 + time_slot_3 + time_slot_4
                time_slots.append(
                    tuple((1, round(time_slot_1/slot_count*100, 2)))
                )
                time_slots.append(
                    tuple((2, round(time_slot_2/slot_count*100, 2)))
                )
                time_slots.append(
                    tuple((3, round(time_slot_3/slot_count*100, 2)))
                )
                time_slots.append(
                    tuple((4, round(time_slot_4/slot_count*100, 2)))
                )
                time_slots = arr_to_str(time_slots)

                """
                'year', 'month', 'avg_trip_duration', 'avg_trip_distance', 
                'gender_share', 'age_share', 'top_used_bikes', 
                'top_start_stations', 'top_end_stations', 'time_slots'
                """
                rows.append(
                    (year, month, avg_trip_duration, avg_trip_distance, gender_share, age_share, top_used_bikes, top_start_stations, top_end_stations, time_slots)
                )

        print('Create Final CSV')
        rdd = spark.sparkContext.parallelize(rows)
        final_df = rdd.toDF(columns)
        
        final_df.coalesce(1)\
                    .write\
                    .format('csv')\
                    .option("header", "true")\
                    .mode('overwrite')\
                    .save('/user/hadoop/hubway/kpis/hubway-kpis.csv')
        print('All done')
