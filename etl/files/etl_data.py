import pandas as pd
import sqlite3
#Extract data from the Files

import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)

def extract(file_path):
    """
    Extract data from a CSV file.
    """
    try:
        data = pd.read_csv(file_path)
        logging.info("Data extracted successfully.")
        return data
    except Exception as e:
        logging.error(f"Error in extraction: {e}")
        return None

def transform(data):
    """
    Transform the data by handling missing values and adding new features.
    """
    try:
        # Check for missing values
        logging.info("Checking for missing values...")
        missing_values = data.isnull().sum()
        logging.info(f"Missing values before transformation:\n{missing_values}")

        # Fill missing 'Income' values based on expenses
        null_income_rows = data[data['Income'].isnull()]
        print("Rows with missing 'Income':")
        print(null_income_rows)

        expense_columns = [
            'MntWines', 'MntFruits', 'MntMeatProducts',
            'MntFishProducts', 'MntSweetProducts', 'MntGoldProds'
        ]

        # Calculate total expenses
        data['Total_Expense'] = data[expense_columns].sum(axis=1)

        # Compute the average ratio of Income to Total Expense
        valid_data = data.dropna(subset=['Income'])
        avg_income_expense_ratio = (valid_data['Income'] / valid_data['Total_Expense']).mean()

        # Fill missing 'Income' values
        data.loc[data['Income'].isnull(), 'Income'] = data['Total_Expense'] * avg_income_expense_ratio

        # Confirm no more missing values
        missing_values_after = data.isnull().sum()
        logging.info(f"Missing values after transformation:\n{missing_values_after}")

        logging.info("Data transformed successfully.")
        return data

    except Exception as e:
        logging.error(f"Error in transformation: {e}")
        return None

def load(data, db_name, table_name):
    """
    Load the transformed data into a SQLite database.
    """
    try:
        conn = sqlite3.connect(db_name)
        data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logging.info(f"Data loaded into the database '{db_name}' successfully.")
    except Exception as e:
        logging.error(f"Error in loading data: {e}")

# Usage example
if __name__ == "__main__":
    file_path = 'etl/files/superstore_data.csv'
    db_name = 'etl_database.db'
    table_name = 'transformed_data'

    # Extract
    extracted_data = extract(file_path)

    # Transform
    if extracted_data is not None:
        transformed_data = transform(extracted_data)

        # Load
        if transformed_data is not None:
            load(transformed_data, db_name, table_name)

            # Verify the loaded data
            conn = sqlite3.connect(db_name)
            query = f"SELECT * FROM {table_name}"
            result = pd.read_sql(query, conn)
            conn.close()

            print("Data from the database:")
            print(result.head())
