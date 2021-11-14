hiveSQL_add_partition_hubway_data='''
ALTER TABLE hubway_data
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '{path}{folder}/hubway-tripdata.csv';
'''