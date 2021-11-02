import os
import kaggle

kaggle.api.authenticate()
kaggle.api.dataset_download_files(
    dataset='acmeyer/hubway-data', 
    path='hubway_data',
    force=False,
    unzip=True
)
