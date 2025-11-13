import logging
import numpy as np
import pandas as pd 

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

    # Define expected schema to enforce data quality and consistency. 
    expected_cols = [
        'Transaction_Id',
         'Customer_Id',
        'Category',
        'Item',
        'Price_Per_Unit',
        'Quantity',
        'Total_Spent',
        'Payment_Method',
        'Location',
        'Transaction_Date',
        'Discount_Applied'
    ]
    
    # Check for discrepancies by comparing current columns against the expected schema. 
    missing_columns = [col for col in expected_cols if col not in df.columns]
    extra_columns = [col for col in df.columns if col not in expected_cols]

    if missing_columns:
        logging.error(f'[Transform][rename_columns] Missing columns: {missing_columns}')
        raise ValueError(f'[Transform][rename_columns] The dataframe schema is invalid. The following columns are missing: {missing_columns}')
    
    if extra_columns: 
        logging.info(f'[Transform][rename_columns] Extra columns found: {extra_columns}. Returning only expected columns')

    # Debug log of final columns 
    logging.debug(f'[Transform][rename_columns] Columns after renaming: {list(df.columns)}')

    return df

def data_overview(df: pd.DataFrame, stage: str = "INITIAL", invalid_values: list = None) -> dict:
    """
    Logs missing values and basic data quality statistics before and after core transformation steps.
    """
    # Create a copy to ensure the function is pure.
    df_copy = df.copy()

    # Define common string representations of missing/invalid data to be standardized.
    if invalid_values is None:
        invalid_values = ['error', 'unknown', 'nan', 'none', 'na', '']

    # Define explicitly the columns that are expected to be strings/categorical data.
    string_cols = [
        'Transaction_Id', 
        'Customer_Id', 
        'Category', 
        'Item', 
        'Payment_Method', 
        'Location'
    ]

    # Standardize string columns: convert common invalid values to np.nan for accurate counting.
    for col in string_cols:
        if col in df.columns:
            df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(invalid_values, np.nan)
        )
            
    # Compute missing value summary across all columns after preliminary cleaning.
    missing_summary = df_copy.isna().sum()
    
    # Log the results, including the current stage (INITIAL or FINAL) for traceability.
    logging.info(f'[Transform][data_overview][{stage}] Missing values per column:\n{missing_summary}')

    # Assemble a dictionary with key data quality statistics for potential return or storage.
    stats = {
        'row_count': len(df_copy), 
        'missing_values': missing_summary.to_dict(), 
        'data_type': df_copy.dtypes.apply(str).to_dict()
    }

    return stats

def transform_data(df):
    """
    Main transformation pipeline. 
    """
    # === STEP 0: CREATE A COPY OF THE DATAFRAME ===
    df_clean = df.copy

    # === STEP 1: RENAMING & VALIDATE SCHEMA === 
    logging.info(f'[Transform][rename_columns] Starting columns standardization and schema validation.')
    df_clean = rename_columns(df_clean)

    # === STEP 2: LOG DATA QUALITY (INITIAL) ===
    logging.info('[Transform][data_overview] Logging initial data statistics.')
    df_clean = data_overview(df_clean, stage='INITIAL')

    return df_clean
