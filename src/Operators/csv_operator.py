import pandas as pd
import ast
import os

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator

from Helper.coordinates_helper import distanceInKmBetweenEarthCoordinates

class CSVOptimizingOperator(BaseOperator):
    
    @apply_defaults
    def __init__(
        self, 
        local_input_path: str, 
        local_output_path: str, 
        file_names,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.local_input_path = local_input_path
        self.local_output_path = local_output_path
        self.file_names = file_names

    def execute(self, context):
        
        self.log.info("Start optimizing csv")
        
        file_names = ast.literal_eval(self.file_names[0])

        for file_name in file_names:
            file = os.path.join(self.local_input_path, file_name)
            new_file_name = file_name.split('.')[0] + '-final' + '.csv'
            output_file = os.path.join(self.local_output_path, new_file_name)

            # Read CSV
            data = pd.read_csv(file)

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
            data.to_csv(output_file)

        self.log.info("Done optimizing csv")
