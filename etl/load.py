import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def get_engine():
    """
    Create a SQLAlchemy engine for Azure PostgreSQL Flexible Server.

    Reads connection params from environment variables injected by Docker.
    SSL is required by Azure PostgreSQL — the connect_args enforce it.

    Returns:
        sqlalchemy.engine.Engine or None
    """
    try:
        user     = os.environ["ETL_DB_USER"]
        password = os.environ["ETL_DB_PASSWORD"]
        host     = os.environ["ETL_DB_HOST"]
        port     = os.environ.get("ETL_DB_PORT", "5432")
        dbname   = os.environ["ETL_DB_NAME"]

        connection_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )

        # Azure PostgreSQL Flexible Server requiere SSL obligatoriamente
        engine = create_engine(
            connection_string,
            connect_args={"sslmode": "require"},
        )
        return engine

    except KeyError as e:
        print(f"Missing environment variable: {e}")
        return None
    except SQLAlchemyError as e:
        print(f"Error creating engine: {e}")
        return None


def ensure_schema(engine, schema: str):
    """
    Creates the schema if it doesn't exist yet.
    Safe to call on every run — CREATE SCHEMA IF NOT EXISTS is idempotent.

    Args:
        engine: SQLAlchemy engine
        schema: schema name (e.g. "crypto")
    """
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    print(f"Schema '{schema}' is ready.")


def load_data(df: pd.DataFrame):
    """
    Persists a cleaned DataFrame into Azure PostgreSQL under the project schema.

    The schema is read from ETL_DB_SCHEMA env var (default: "crypto").
    This lets each project write to its own isolated schema on the same server:
        crypto   → crypto.crypto_prices
        weather  → weather.observations
        ...

    Args:
        df: cleaned DataFrame from transform step
    """
    if df.empty:
        print("No data to load: DataFrame is empty.")
        return

    schema = os.environ.get("ETL_DB_SCHEMA", "crypto")

    engine = get_engine()
    if engine is None:
        print("Skipping load: could not create engine.")
        return

    try:
        # Garantiza que el schema existe antes de insertar
        ensure_schema(engine, schema)

        df.to_sql(
            name="crypto_prices",
            schema=schema,          # <- apunta al schema del proyecto
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        print(
            f"Loaded {len(df)} rows into '{schema}.crypto_prices' "
            f"on {os.environ['ETL_DB_HOST']}"
        )

    except SQLAlchemyError as e:
        print(f"SQLAlchemy error: {e}")
        raise  # Re-raise para que Airflow marque la task como failed
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        engine.dispose()
        print("Engine disposed.")
