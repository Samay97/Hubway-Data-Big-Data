import pandas as pd

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator

from Helper.coordinates_helper import distanceInKmBetweenEarthCoordinates

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
        self.log.info("Start optimizing csv")

        # Read CSV
        data = pd.read_csv('./input/201501-hubway-tripdata.csv')

        # Create new col
        data['triptripdistance'] = '0'
        
        # Update date in each row
        for i, row in data.iterrows():
                data.at[i,'triptripdistance'] = distanceInKmBetweenEarthCoordinates(
                    row['start station latitude'],
                    row['start station longitude'],
                    row['end station latitude'],
                    row['end station longitude']
                )
        
        # Remove col
        data = data.drop('start station latitude', axis=1)
        data = data.drop('start station longitude', axis=1)
        data = data.drop('end station latitude', axis=1)
        data = data.drop('end station longitude', axis=1)
        data = data.drop('usertype', axis=1)
        
        # Safe CSV
        data.to_csv('./output/201501-hubway-tripdata.csv')

        self.log.info("Done optimizing csv")
