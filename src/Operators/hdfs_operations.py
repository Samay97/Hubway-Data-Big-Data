from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from pendulum import local

from Hook.hdfs_hook import HdfsHook


class HdfsPutFileOperator(BaseOperator):

    template_fields = ('local_path', 'local_files', 'remote_path', 'remote_files', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            local_path,
            local_files,
            remote_path,
            remote_files,
            hdfs_conn_id,
            *args, **kwargs):

        super(HdfsPutFileOperator, self).__init__(*args, **kwargs)
        self.local_files = local_files
        self.local_path = local_path
        self.remote_files = remote_files
        self.remote_path = remote_path
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsPutFileOperator execution started")


        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)

        for x in range(len(self.local_files)):
            local_file = self.local_files[x]
            remote_file = self.remote_files[x]
            
            self.log.info("Upload file '" + local_file + "' to HDFS '" + remote_file + "'.")
        
            hh.putFile(local_file, remote_file)

        self.log.info("HdfsPutFileOperator done")
