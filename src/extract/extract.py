import pandas as pd
import logging


def extract_data(file_path):
    """
    EXTRACT DATA FROM CSV AND LOG DETAILED INFO
    """
    # === STEP 1: READ CSV ===
    logging.debug(f'[Extract] Attempting to read CSV file from {file_path}')
    
    try:
        df_raw = pd.read_csv(file_path)

    # === STEP 2: DATA QUALITY STATS ===
    
        total_rows = df_raw.shape[0]
        total_columns = df_raw.shape[1]
        dtype_info = df_raw.dtypes.to_dict()
        missing_values = df_raw.isna().sum().sum()

        stats = {
            'rows': total_rows, 
            'columns': total_columns, 
            'missing values': missing_values, 
            'dtype': dtype_info
        }

    # === STEP 3 : LOG STATS ===
        logging.info(f'[Extract] Data successfully extracted from {file_path}')
        logging.info(f'[Extract] Rows: {total_rows}, Columns: {total_columns}, Missing Values: {missing_values}')
        logging.debug(f'[Extract] Column Types: {dtype_info}')
        return df_raw, stats
    
    except FileNotFoundError:
    
        logging.error(f'[Extract] FAILED: File not found at path: {file_path}')
