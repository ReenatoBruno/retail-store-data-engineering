import os
import pandas as pd 

# PATH OF THE BASE DIRECTORY
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# PATH OF THE DATA FOLDER
DATA_PATH = os.path.join(BASE_PATH, 'data')

# SUBFOLDERS PATHS
DIRS = {name: os.path.join(DATA_PATH, name) for name in ['raw', 'processed','backup']}
RAW_DIRS = DIRS['raw']
PROCESSED_DIRS = DIRS['processed']
BACKUP_DIRS = DIRS['backup']

# MAIN FILE NAME AND PATH
RAW_FILE_NAME = 'retail_store.csv'
RAW_FILE_PATH = os.path.join(RAW_DIRS, RAW_FILE_NAME)
