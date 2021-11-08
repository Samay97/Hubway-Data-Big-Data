hiveSQL_create_table_hubway_data='''
CREATE EXTERNAL TABLE IF NOT EXISTS hubway_data(
	tripduration INT,
	starttime TIMESTAMP,
	stoptime TIMESTAMP,
	start_station_id INT,
	start_station_name STRING,
	start_station_latitude STRING,
	start_station_longitude STRING,
	end_station_id INT,
    end_station_name STRING,
    end_station_latitude STRING,
	end_station_longitude STRING,
    bikeid INT,
    usertype STRING,
    birth_year DECIMAL(4,0),
    gender DECIMAL(1,0)
) COMMENT 'Hubway Data' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_create_table_hubway_data_optimized='''
CREATE EXTERNAL TABLE IF NOT EXISTS hubway_data(
	tripduration INT,
	tripdistance INT,
	starttime TIMESTAMP,
	stoptime TIMESTAMP,
	start_station_id INT,
	start_station_name STRING,
	end_station_id INT,
    end_station_name STRING,
    bikeid INT,
    birth_year DECIMAL(4,0),
    gender DECIMAL(1,0)
) COMMENT 'Hubway Data' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
'''
