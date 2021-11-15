hiveSQL_create_table_hubway_data_optimized='''
CREATE EXTERNAL TABLE IF NOT EXISTS hubway_data(
	tripduration INT,
	start_station_id INT,
	start_station_name STRING,
	end_station_id INT,
	end_station_name STRING,
	bikeid INT,
	gender DECIMAL(1,0),
	tripdistance FLOAT,	
    age DECIMAL(2,0),
	timeslot_1 DECIMAL(1,0),
	timeslot_1 DECIMAL(1,0),
	timeslot_2 DECIMAL(1,0),
	timeslot_3 DECIMAL(1,0),
	timeslot_4 DECIMAL(1,0)
) COMMENT 'Hubway Data' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u002C' STORED AS TEXTFILE LOCATION '/user/hadoop/hubway/sql'
TBLPROPERTIES ('skip.header.line.count'='1');
'''
