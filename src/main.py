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
from Operators.filesystem_operator import (ClearDirectoryOperator,
                                           CreateDirectoryOperator,
                                           RemoveFileOperator)
from Operators.hdfs_operations import (HdfsBasicMkdirFileOperator,
                                       HdfsMkdirFileOperator,
                                       HdfsPutFileOperator)
from Operators.hive_operator import HiveUploadOperator
from SQL_Commands.create_table import \
    hiveSQL_create_table_hubway_data_optimized
from SQL_Commands.insert_to_table import hiveSQL_add_partition_hubway_data
from SQL_Commands.select_commands import hiveSQL_select_AVG_trip_duration

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
    hdfs_conn_id=hdfs_conn_id,
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(get_downloaded_filenames)


create_hdfs_hubway_data_partition_dir_final = HdfsMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-final',
    directory=remote_path_final,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id=hdfs_conn_id,
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_final.set_upstream(get_downloaded_filenames)


create_hdfs_hubway_data_partition_dir_hiveSQL = HdfsBasicMkdirFileOperator(
    task_id='mkdir-hdfs-hubway-data-dir-hiveSQL',
    directory=remote_path_hivesql,
    hdfs_conn_id=hdfs_conn_id,
    dag=dag,
)

hdfs_put_hubway_data_raw = HdfsPutFileOperator(
    task_id='upload-hubway-data-to-hdfs-raw',
    local_path='{}/{}'.format(download_path, download_folder),
    remote_path=remote_path_raw,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    hdfs_conn_id=hdfs_conn_id,
    dag=dag,
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

# def load_subdag(parent_dag_name, child_dag_name, file_names, args):
#     dag_subdag = DAG(
#         dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
#         default_args=args,
#         schedule_interval='0 18 * * *',
#         start_date=datetime(2021, 11, 1)
#     )
#     with dag_subdag:
#         for i in file_names:
#             t = DummyOperator(
#                 task_id='load_subdag_{0}'.format(i),
#                 dag=dag_subdag,
#             )

#     return dag_subdag

# load_tasks = SubDagOperator(
#     task_id="load_tasks",
#     subdag=load_subdag(
#         parent_dag_name='Hubway-Data',
#         child_dag_name="load_tasks",
#         file_names="{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}",
#         args=args
#     ),
#     dag=dag
# )


# TODO: Don´t know how to get dynmaic HiveOperators with {{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}
# last_submit = None
# for x in ['201512', '201506', '201705', '201702', '201703', '201712', '201709', '201710', '201504', '201606', '201708',
# '201507', '201609', '201508', '201605', '201704', '201505', '201706', '201701', '201601', '201603', '201608', '201510',
# '201602', '201509', '201707', '201607', '201604', '201502', '201511', '201503', '201610', '201711', '201501', '201611']:
#     upload_to_hive_database = HiveOperator(
#         task_id='upload_to_hive_sql_{}'.format(x),
#         hql=hiveSQL_add_partition_hubway_data + " LOCATION '{}{}/hubway-tripdata.csv'; ".format(remote_path_final, x),
#         hive_cli_conn_id='beeline',
#         dag=dag
#     )

#     if last_submit is not None:
#         upload_to_hive_database.set_upstream(last_submit)
#     else:
#         upload_to_hive_database.set_upstream(waiting_operator_two)
    
#     upload_to_hive_database.set_downstream(waiting_operator_three)
#     last_submit = upload_to_hive_database


upload_to_hive_database = HiveUploadOperator(
    task_id='upload_to_hive_database',
    hql=hiveSQL_add_partition_hubway_data,
    file_names=["{{ task_instance.xcom_pull(task_ids='get-downloaded-filenames') }}"],
    remote_path_final=remote_path_final,
    hive_cli_conn_id='beeline',
    dag=dag
)

hive_get_avg_trip_duration = HiveOperator(
    task_id='hiveSQL_select_AVG_trip_duration',
    hql=hiveSQL_select_AVG_trip_duration,
    hive_cli_conn_id='beeline',
    dag=dag
)

create_local_import_dir >> clear_local_import_dir >> download_dataset >> get_downloaded_filenames
download_dataset >> remove_zip_file_from_download
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_raw >> hdfs_put_hubway_data_raw >> waiting_operator
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_final >> waiting_operator
get_downloaded_filenames >> create_hdfs_hubway_data_partition_dir_hiveSQL >> create_HiveTable_hubway_data >> waiting_operator_two
waiting_operator >> csv_optimize >> waiting_operator_two >> upload_to_hive_database >> hive_get_avg_trip_duration
