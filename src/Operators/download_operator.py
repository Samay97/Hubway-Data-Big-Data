import kaggle

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator


class DownloadOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, dataset: str, path: str, **kwargs) -> None:
        """
        :param dataset: dataeet to download
        :type dataset: string
        :param path: where to download the zip file to
        :type path: string
        """
        super().__init__(**kwargs)
        self.dataset = dataset
        self.path = path

    def execute(self, context):
        kaggle.api.authenticate()

        self.log.info("authenticate sucess")
        self.log.info("Dataset download started")

        kaggle.api.dataset_download_files(
            dataset='acmeyer/hubway-data', 
            path=self.path,
            force=False,
            unzip=True
        )

        self.log.info("Dataset download done")
