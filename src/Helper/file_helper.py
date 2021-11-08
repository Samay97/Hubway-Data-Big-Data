import re
import os

from glob import glob
from sys import path
from config import *

 
def collect_all_downloaded_files(**kwargs):
    path = os.path.join(download_path, download_folder)
    csv_files = glob(path + '/*.csv')
        
    # Filter just trip data
    regex = re.compile(path + '[a-zA-Z\/\_]*[0-9]{6}-[a-z]+-[a-z]+.csv')
    filtered_csv_files = [ x for x in csv_files if regex.match(x)]
    file_names = [ os.path.basename(x) for x in filtered_csv_files]

    return file_names


def get_folder_to_create(directory, files):
    folders_to_create = []    
    for file in files:
        folder_name = file.split('-')[0]
        if not folder_name in folders_to_create:
            folders_to_create.append(folder_name)

    return [ os.path.join(directory, folder) for folder in folders_to_create]
