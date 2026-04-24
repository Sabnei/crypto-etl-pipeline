import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def get_engine():
    """
    Create a SQLAlchemy engine for PostgreSQL using environment variables.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object if successful, None otherwise.
    """
    try:
        user = os.getenv("ETL_DB_USER")
        password = os.getenv("ETL_DB_PASSWORD")
        host = os.getenv("ETL_DB_HOST")
        port = os.getenv("ETL_DB_PORT")
        dbname = os.getenv("ETL_DB_NAME")

        # Connection string: postgresql://user:password@host:port/dbname
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        return create_engine(connection_string)
    except SQLAlchemyError as e:
        print(f"Error creating SQLAlchemy engine: {e}")
        return None
    
def load_data(df):
    """
    Persists DataFrame records into the PostgreSQL database using pandas to_sql.
    
    This method leverages SQLAlchemy for a more efficient and cleaner insertion
    process compared to manual cursor execution.

    Args:
        df (pandas.DataFrame): Dataframe containing cleaned crypto data.
    """
    if df.empty:
        print("No data to load: DataFrame is empty.")
        return
    
    engine = get_engine()
    if engine is None:
        print("Skipping data load: Could not create database engine.")
        return
    
    try:
        # if_exists='append' ensures we add new rows without dropping the table
        # method='multi' improves performance by sending multiple rows in one INSERT
        # index=False prevents pandas from adding the DF index as a column
        df.to_sql(
            name='crypto_prices', 
            con=engine, 
            if_exists='append', 
            index=False, 
            method='multi'
        )
        print(f"Successfully loaded {len(df)} rows into 'crypto_prices' table using to_sql.")

    except SQLAlchemyError as e:
        print(f"SQLAlchemy error during data insertion: {e}")
    except Exception as e:
        print(f"Unexpected error during load: {e}")
    finally:
        # Engine disposal is good practice to release connection pool resources
        engine.dispose()
        print("Database connection closed.")
