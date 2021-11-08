from datetime import datetime
from airflow import DAG

# Config
from config import *

# Helpers
from Helper.file_helper import collect_all_downloaded_files

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from Operators.download_operator import DownloadOperator
from Operators.filesystem_operator import CreateDirectoryOperator, ClearDirectoryOperator, RemoveFileOperator
from Operators.hdfs_operations import HdfsMkdirFileOperator, HdfsPutFileOperator
from Operators.csv_operator import CSVOptimizingOperator

# Sql commands
from SQL_Commands.create_table import hiveSQL_create_table_hubway_data



args = {
    'owner': 'airflow'
}

dag = DAG(
        'Hubway-Data',
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

get_downloaded_filenames = PythonOperator(
    task_id='get-downloaded-filenames',
    provide_context=True,
    python_callable=collect_all_downloaded_files
)

create_hdfs_hubway_data_partition_dir_raw = HdfsMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-raw',
    directory=remote_path_raw,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(get_downloaded_filenames)


create_hdfs_hubway_data_partition_dir_final = HdfsMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-final',
    directory=remote_path_final,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(get_downloaded_filenames)

hdfs_put_hubway_data_raw = HdfsPutFileOperator(
    task_id='upload-hubway-data-to-hdfs-raw',
    local_path='{}/{}'.format(download_path, download_folder),
    remote_path=remote_path_raw,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_hubway_data_raw.set_upstream(get_downloaded_filenames)


csv_optimizing = CSVOptimizingOperator(
    task_id='upload-hubway-data-to-hdfs-raw',
    local_input_path='{}/{}'.format(download_path, download_folder),
    local_output_path='{}/{}'.format(download_path, download_folder),
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    dag=dag,
)

csv_optimizing.set_upstream(get_downloaded_filenames)


# TODO get final files from donwload folder and upload them
hdfs_put_hubway_data_final = HdfsPutFileOperator(
    task_id='upload-hubway-data-to-hdfs-final',
    local_path='{}/{}'.format(download_path, download_folder),
    remote_path=remote_path_raw,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_hubway_data_final.set_upstream(get_downloaded_filenames)

waiting_operator = DummyOperator(
    task_id='Dummy_Task_Wait',
    dag=dag
)

waiting_operator2 = DummyOperator(
    task_id='Dummy_Task_Wait 2',
    dag=dag
)

create_local_import_dir >> clear_local_import_dir >> download_dataset >> remove_zip_file_from_download >> get_downloaded_filenames
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_raw >> waiting_operator
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_final >> waiting_operator
get_downloaded_filenames >> csv_optimizing >> waiting_operator2
waiting_operator >> hdfs_put_hubway_data_raw
waiting_operator2 >> hdfs_put_hubway_data_final
