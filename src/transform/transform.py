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


def transform_data(df):
    """
    Main transformation pipeline. 
    """

    # Create a copy to ensure the function is pure.
    df_clean = df.copy

    # Renaming and Schema validation step.
    logging.info(f'[Transform][rename_columns] Starting columns standardization and schema validation.')
    df_clean = rename_columns(df_clean)


    return df_clean
