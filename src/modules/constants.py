EXPECTED_COLS = [
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

# Define explicitly the columns that are expected to be strings/categorical data.
STRING_COLS = [
    'Transaction_Id', 
    'Customer_Id', 
    'Category', 
    'Item', 
    'Payment_Method', 
    'Location'
]

# Define explicitly the columns that are expected to be numeric data. 
NUMERIC_COLS = [
    'Price_Per_Unit', 
    'Quantity', 
    'Total_Spent'
]
