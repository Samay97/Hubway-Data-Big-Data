import re
import ast

from glob import glob
from os import path

from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.sql.sqltypes import String

from Hook.hdfs_hook import HdfsHook
from Helper.file_helper import get_folder_to_create


class HdfsMkdirFileOperator(BaseOperator):

    template_fields = ('directory', 'hdfs_conn_id', 'file_names')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            directory,
            hdfs_conn_id,
            file_names,
            *args, **kwargs):
        """
        :param directory: directory, which should be created
        :type directory: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsMkdirFileOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.hdfs_conn_id = hdfs_conn_id
        self.file_names = file_names

    def execute(self, context):

        self.log.info("HdfsMkdirFileOperator execution started")

        file_names = ast.literal_eval(self.file_names[0])
        folders_to_create = get_folder_to_create(self.directory, file_names)
        
        for dicrectory in folders_to_create:
            self.log.info("Mkdir HDFS directory'" + dicrectory + "'.")
            hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
            hh.mkdir(dicrectory)

        self.log.info("HdfsMkdirFileOperator done")


class HdfsPutFileOperator(BaseOperator):

    template_fields = ('local_path', 'remote_path', 'file_names', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            local_path,
            remote_path,
            file_names,
            hdfs_conn_id,
            *args, **kwargs):

        super(HdfsPutFileOperator, self).__init__(*args, **kwargs)
        self.local_files = []
        self.remote_files = []
        self.local_path = local_path
        self.remote_path = remote_path
        self.file_names = file_names
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsPutFileOperator execution started")
        
        file_names = ast.literal_eval(self.file_names[0])
        self.local_files = [ path.join(self.local_path, file) for file in file_names]
        #get_folder_to_create(self.local_path, file_names)
        self.remote_files = [path.join(folder, 'hubway-tripdata.csv') for folder in get_folder_to_create(self.remote_path, file_names)]

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)

        for index, file in enumerate(self.local_files):
            local_file = file
            remote_file = self.remote_files[index]
            
            self.log.info("Upload file '" + local_file + "' to HDFS '" + remote_file + "'.")
        
            hh.putFile(local_file, remote_file)

        self.log.info("HdfsPutFileOperator done")
