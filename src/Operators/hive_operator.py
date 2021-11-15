import ast
from typing import Any, Dict

from airflow.operators.hive_operator import HiveOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class HiveUploadOperator(HiveOperator):

    template_fields = ('file_names', 'remote_path_final')

    @apply_defaults
    def __init__(
            self,
            file_names,
            remote_path_final,
            *args, **kwargs):

        super(HiveUploadOperator, self).__init__(*args, **kwargs)
        self.file_names = file_names
        self.remote_path_final = remote_path_final

    
    def execute(self, context: Dict[str, Any]) -> None:
        
        file_names = ast.literal_eval(self.file_names[0])
        base_hql = self.hql

        for file in file_names:
            self.hql =base_hql + " LOCATION '{}{}/hubway-tripdata.csv'; ".format(self.remote_path_final, file)
            super().execute(context)
