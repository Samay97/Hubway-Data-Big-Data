import ast
from datetime import datetime
from sys import path

from airflow import DAG
from airflow.configuration import get
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from config import *
from Helper.file_helper import collect_all_downloaded_files
from Operators.download_operator import DownloadOperator
from Operators.excel_operator import CreateExcelFromCSV
from Operators.filesystem_operator import (ClearDirectoryOperator,
                                           CreateDirectoryOperator,
                                           RemoveFileOperator)
from Operators.hdfs_operations import (HdfsBasicMkdirFileOperator,
                                       HdfsGetCSVFileOperator,
                                       HdfsMkdirFileOperator,
                                       HdfsPutFileOperator)
from Operators.hive_operator import HiveUploadOperator
from SQL_Commands.create_table import \
    hiveSQL_create_table_hubway_data_optimized
from SQL_Commands.insert_to_table import hiveSQL_add_partition_hubway_data

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

waiting_operator = DummyOperator(
    task_id='wait_for_upload_hdfs',
    dag=dag
)

waiting_operator_two = DummyOperator(
    task_id='Dummy_Task_Wait_two',
    dag=dag
)

waiting_operator_three = DummyOperator(
    task_id='Dummy_Task_Wait_three',
    dag=dag
)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path=download_path,
    directory=download_folder,
    dag=dag
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='{}/{}'.format(download_path, download_folder),
    pattern='*',
    dag=dag
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
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(get_downloaded_filenames)


create_hdfs_hubway_data_partition_dir_final = HdfsMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-final',
    directory=remote_path_final,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

create_hdfs_hubway_data_partition_dir_final.set_upstream(get_downloaded_filenames)


create_hdfs_hubway_data_partition_dir_hiveSQL = HdfsBasicMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-hiveSQL',
    directory=remote_path_hivesql,
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

create_hdfs_hubway_data_partition_dir_kpis = HdfsBasicMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-kpis',
    directory=remote_path_kpis,
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

hdfs_put_hubway_data_raw = HdfsPutFileOperator(
    task_id='upload-hubway-data-to-hdfs-raw',
    local_path='{}/{}'.format(download_path, download_folder),
    remote_path=remote_path_raw,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

csv_optimize = SparkSubmitOperator(
        task_id='pyspark_csv_optimize',
        conn_id='spark',
        application='/home/airflow/airflow/dags/PySpark/csv_optimize.py',
        total_executor_cores='4',
        executor_cores='2',
        executor_memory='4g',
        num_executors='2',
        name='spark_optimize_csv',
        verbose=True,
        application_args=['--filenames', "{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
        dag=dag
)

create_HiveTable_hubway_data = HiveOperator(
    task_id='create_hubway_data_table',
    hql=hiveSQL_create_table_hubway_data_optimized,
    hive_cli_conn_id='beeline',
    dag=dag
)

upload_to_hive_database = HiveUploadOperator(
    task_id='upload_to_hive_database',
    hql=hiveSQL_add_partition_hubway_data,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    remote_path_final=remote_path_final,
    hive_cli_conn_id='beeline',
    dag=dag
)

calculate_kpis = SparkSubmitOperator(
        task_id='pyspark_calculate_kpis',
        conn_id='spark',
        application='/home/airflow/airflow/dags/PySpark/calculate_kpis.py',
        total_executor_cores='4',
        executor_cores='2',
        executor_memory='6g',
        num_executors='2',
        name='calculate_kpis',
        verbose=True,
        application_args=['--filenames', "{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
        dag=dag
)

get_calculated_kpis = HdfsGetCSVFileOperator(
    task_id='get_calculated_kpis',
    remote_file='{}{}'.format(remote_path_kpis, kpis_file_name),
    local_file='{}{}/{}'.format(local_path_kpis, local_kpis_folder, kpis_file_name),
    hdfs_conn_id=hdfs_conn_id,
    dag=dag
)

create_local_kpis_dir = CreateDirectoryOperator(
    task_id='create_kpis_dir',
    path=local_path_kpis,
    directory=local_kpis_folder,
    dag=dag
)

clear_local_kpis_dir = ClearDirectoryOperator(
    task_id='clear_kpis_dir',
    directory='{}{}'.format(local_path_kpis, local_kpis_folder),
    pattern='*',
    dag=dag
)

create_final_excel_kpis = CreateExcelFromCSV(
    task_id='create_final_excel_kpis',
    csv_path='{}{}/{}'.format(local_path_kpis, local_kpis_folder, kpis_file_name),
    excel_path='/home/airflow/excel-files/',
    dag=dag
)

create_local_import_dir >> clear_local_import_dir >> download_dataset >> get_downloaded_filenames
download_dataset >> remove_zip_file_from_download
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_raw >> hdfs_put_hubway_data_raw >> waiting_operator
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_final >> waiting_operator
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_hiveSQL >> create_HiveTable_hubway_data >> waiting_operator_two
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_kpis >> waiting_operator
waiting_operator >> csv_optimize >> waiting_operator_two
waiting_operator_two >> calculate_kpis >> waiting_operator_three
waiting_operator_two >> create_local_kpis_dir >> clear_local_kpis_dir >> waiting_operator_three
waiting_operator_two >> upload_to_hive_database
waiting_operator_three >> get_calculated_kpis >> create_final_excel_kpis

