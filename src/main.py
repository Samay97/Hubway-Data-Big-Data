from datetime import datetime
from airflow import DAG

# Operators
from Operators.download_operator import DownloadOperator
from Operators.filesystem_operator import CreateDirectoryOperator, ClearDirectoryOperator, RemoveFileOperator

from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator

# Sql commands
from SQL_Commands.create_table import hiveSQL_create_table_hubway_data

download_path = '/home/airflow'
download_folder = 'hubway_data'

args = {
    'owner': 'airflow'
}

dag = DAG('Hubway-Data',
        default_args=args,
        description='Hubway-Data ...',
        schedule_interval='0 18 * * *',
        start_date=datetime(2021, 11, 1),
        catchup=False,
        max_active_runs=1
)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path=download_path,
    directory=download_folder,
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='{}/{}'.format(download_path, download_folder),
    pattern='*',
    dag=dag,
)

download_dataset = DownloadOperator(
    task_id='download-task-id',
    dataset='acmeyer/hubway-data', 
    path='{}/{}'.format(download_path, download_folder),
    dag=dag
)

remove_zip_file_from_download = RemoveFileOperator(
    task_id='remove-zip-file',
    path='{}/{}'.format(download_path, download_folder),
    file='hubway-data.zip',
    dag=dag
)

create_hdfs_hubway_data_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir',
    directory='/user/hadoop/hubway_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#hdfs_put_title_ratings = HdfsPutFileOperator(
#    task_id='upload-hubway-data-to-hdfs',
#    local_file='/home/airflow/imdb/title.ratings_{{ ds }}.tsv',
#    remote_file='/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/title.ratings_{{ ds }}.tsv',
#    hdfs_conn_id='hdfs',
#    dag=dag,
#)

create_local_import_dir >> clear_local_import_dir 
clear_local_import_dir >> download_dataset >> remove_zip_file_from_download
remove_zip_file_from_download >> create_hdfs_hubway_data_partition_dir
