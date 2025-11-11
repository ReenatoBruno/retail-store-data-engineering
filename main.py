import logging
import pandas as pd 
from src.extract.extract import extract_data
from src.log.log_config import setup_logging
from src.modules.paths import RAW_FILE_PATH

setup_logging()

def main():

    logging.info(f'[Pipeline] Starting ETL process...')
    logging.info('='* 50)

    # === STEP 1: EXTRACT ===
    logging.info(f'[Pipeline] Extracting data...')

    df_raw, extract_stats = extract_data(RAW_FILE_PATH)
    
    if df_raw.empty:
        logging.warning(f'[Pipeline] The extracted data is empty. Check the source file.')
        return
    
    logging.info(f'[Pipeline] Data extraction complete!')
    logging.info(f'[Pipeline] First 5 rows of the extracted data:')
    logging.info(f'\n' + df_raw.head().to_string(index=False))
    logging.info('='* 50)


if __name__ == '__main__':
    main()

