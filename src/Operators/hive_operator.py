import ast
from typing import Any, Dict

from airflow.operators.hive_operator import HiveOperator
from airflow.utils.decorators import apply_defaults


class HiveUploadOperator(HiveOperator):

    @apply_defaults
    def __init__(
            self,
            file_names,
            *args, **kwargs):
        """
        :param directory: directory, which should be created
        :type directory: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HiveUploadOperator, self).__init__(*args, **kwargs)
        self.file_names = file_names

    
    def execute(self, context: Dict[str, Any]) -> None:

        self.log.info('....')
        self.log.info(self.file_names)
        file_names = ast.literal_eval(self.file_names)

        for file in file_names:
            print(file)
