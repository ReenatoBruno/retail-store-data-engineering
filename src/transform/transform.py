import logging
import numpy as np
import pandas as pd 
from src.modules.constants import EXPECTED_COLS, STRING_COLS, NUMERIC_COLS

def rename_columns(df):
    """
    Standardize column names and perform schema validation.
    """

    # Create a copy to ensure the function is pure.
    df = df.copy()

    # Apply the cleaning pipeline: strip external spaces, Capitalize, and replace internal spaces with '_'.
    df.columns = (
        df.columns
        .str.strip()
        .str.title()
        .str.replace(' ', '_', regex=False)
    )

    # Check for discrepancies by comparing current columns against the expected schema. 
    missing_columns = [col for col in EXPECTED_COLS if col not in df.columns]
    extra_columns = [col for col in df.columns if col not in EXPECTED_COLS]

    if missing_columns:
        logging.error(f'[Transform][rename_columns] Missing columns: {missing_columns}')
        raise ValueError(f'[Transform][rename_columns] The dataframe schema is invalid. The following columns are missing: {missing_columns}')
    
    if extra_columns: 
        logging.info(f'[Transform][rename_columns] Extra columns found: {extra_columns}. Returning only expected columns')

    # Debug log of final columns 
    logging.debug(f'[Transform][rename_columns] Columns after renaming: {list(df.columns)}')

    return df[EXPECTED_COLS]

def data_overview(df: pd.DataFrame, stage: str = "INITIAL", invalid_values: list = None) -> dict:
    """
    Logs missing values and basic data quality statistics before and after core transformation steps.
    """
    # Create a copy to ensure the function is pure.
    df = df.copy()

    # Define common string representations of missing/invalid data to be standardized.
    if invalid_values is None:
        invalid_values = ['error', 'unknown', 'nan', 'none', 'na', '']

    # Standardize string columns: convert common invalid values to np.nan for accurate counting.
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(invalid_values, np.nan)
        )
    # Standardize numeric columns: convert invalid strings to np.nan for accurate counting. 
    for col in NUMERIC_COLS:  
        if col in df.columns:
            df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace(invalid_values, np.nan)
        )
            
    # Compute missing value summary across all columns after preliminary cleaning.
    missing_summary = df.isna().sum()
    
    # Log the results, including the current stage (INITIAL or FINAL) for traceability.
    logging.info(f'[Transform][data_overview][{stage}] Missing values per column:\n{missing_summary}')

    # Assemble a dictionary with key data quality statistics for potential return or storage.
    stats = {
        'row_count': len(df), 
        'missing_values': missing_summary.to_dict(), 
        'data_type': df.dtypes.apply(str).to_dict()
    }

    return stats

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns to their correct data type. 
    """
    # Create a copy to ensure the function is pure.
    df = df.copy()

    # Standardize string columns by replacing empty spaces with underscores.
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = (
            df[col]
            .astype(str)
            .replace(' ', '_')
        )
            
    # Convert cleaned numeric columns to float64 to turn any remaining invalid values into NaN.
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert the transaction date column to the datetime dtype, coercing any invalid dates to NaT (Not a Time).
    if 'Transaction_Date' in df.columns:
        df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date'], errors='coerce')

    return df

def clean_price_per_unit(df: pd.DataFrame) -> pd.DataFrame:

    # Create a copy to ensure the function is pure.
    df = df.copy()

    # Log the count of NaNs before imputation for quality check.
    count_before_na = df['Price_Per_Unit'].isna().sum()

    # 1. Extract the Item ID (number) from the 'Item' column.
    df['Item_Number'] = (
        df['Item']
        .str.split('_').str[1]
        .astype(str)
    )
    # 2. Create the mapping dictonary (item_id -> Price)
    price_map = (
        df
        .dropna(subset=['Item_Number', 'Price_Per_Unit'])
        .set_index('Item_Number')
        ['Price_Per_Unit'].to_dict()
    )
    # 3. Imputing missing Price_Per_Unit based on Item_Number mapping. 
    df['Price_Per_Unit'] = np.where(
        df['Price_Per_Unit'].isna() & df['Item_Number'].notna(), 
        df['Item_Number'].map(price_map), 
        df['Price_Per_Unit']
    )

    # Log the count of NaNs after imputation for quality check.
    count_after_na = df['Price_Per_Unit'].isna().sum()
    imputed_count = count_before_na - count_after_na
    logging.info(f'[Transform][clean_price_per_unit] Imputed {imputed_count} values using item mapping. {count_after_na} NaNs remain.')

    # Drop the temporaty column.
    df.drop(columns=['Item_Number'], inplace=True)

    return df

def transform_data(df):
    """
    Main transformation pipeline. 
    """
    # === STEP 1: RENAMING & VALIDATE SCHEMA === 
    logging.info(f'[Transform][rename_columns] Starting columns standardization and schema validation.')
    df_clean = rename_columns(df_clean)
    logging.info('='* 50) 

    # === STEP 2: LOG DATA QUALITY (INITIAL) ===
    logging.info('[Transform][data_overview] Logging initial data statistics.')
    data_overview(df_clean, stage='INITIAL') 
    logging.info('='* 50)

    # === STEP 3: DATA TYPE CONVERSION & STANDARDIZATION ===
    logging.info('[Transform][convert_data_types] Initiating data type conversion and text standardization.')
    df_clean = convert_data_types(df_clean)
    logging.info('[Transform][convert_data_types] Data type conversion completed.')
    logging.info('='* 50)

    # === STEP 4: IMPUTE PRICE PER UNIT 
    logging.info('[Transform][clean_price_per_unit] Initiating item-based price imputation.')
    df_clean = clean_price_per_unit(df_clean)
    logging.info('[Transform][clean_price_per_unit] Price imputation finalized.')
    logging.info('='* 50)

    return df_clean
