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

def transform_data(df):
    """
    Main transformation pipeline. 
    """
    # === STEP 0: CREATE A COPY OF THE DATAFRAME ===
    df_clean = df.copy()

    # === STEP 1: RENAMING & VALIDATE SCHEMA === 
    logging.info(f'[Transform][rename_columns] Starting columns standardization and schema validation.')
    df_clean = rename_columns(df_clean)

    # === STEP 2: LOG DATA QUALITY (INITIAL) ===
    logging.info('[Transform][data_overview] Logging initial data statistics.')
    df_clean = data_overview(df_clean, stage='INITIAL')

    return df_clean
