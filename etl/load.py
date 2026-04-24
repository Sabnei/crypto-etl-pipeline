import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from dotenv import load_dotenv
import os

load_dotenv()

INSERT_QUERY = """
    INSERT INTO crypto_prices (coin_id, symbol, name, price_usd, market_cap_usd, 
        volume_24h, change_pct_24h, last_updated_api, extracted_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def get_connection():
    """
    Establishes a connection to the PostgreSQL database using environment variables.

    Retrieves connection parameters (host, port, database name, user, and password)
    from the system environment to create a psycopg2 connection object.

    Returns:
        psycopg2.extensions.connection: A connection object if successful, None otherwise.

    Raises:
        OperationalError: If the database server is unreachable or credentials are invalid.
        InterfaceError: If the database interface encounters a low-level connection issue.
    """
    try:
        return psycopg2.connect(
            host=os.getenv("ETL_DB_HOST"),
            port=os.getenv("ETL_DB_PORT"),
            dbname=os.getenv("ETL_DB_NAME"),
            user=os.getenv("ETL_DB_USER"),
            password=os.getenv("ETL_DB_PASSWORD")
        )
    except OperationalError as e:
        print(f"Operational Error: Could not connect to the server. {e}")
    except InterfaceError as e:
        print(f"Interface error: Library-level connection issue. {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    return None


def load_data(df):
    """
    Persists DataFrame recods into the PostgreSQL database.

    Args:
        df (pandas.DataFrame): Dataframe containig cleaned crypto data.
    """
    conn = get_connection()
    if conn is None:
        print("Skipping data load: No active database connection")
        return

    cursor = None
    try:
        cursor = conn.cursor()

        for row in df.itertuples(index=False):
            cursor.execute(INSERT_QUERY, (
                row.coin_id,
                row.symbol,
                row.name,
                row.price_usd,
                row.market_cap_usd,
                row.volume_24h,
                row.change_pct_24h,
                row.last_updated_api,
                row.extracted_at
            ))
        
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into the database.")

    except DatabaseError as e:
        print(f"Database error during insertion: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Database resources released.")