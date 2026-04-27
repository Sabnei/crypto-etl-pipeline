"""
etl/load.py

Carga los datos transformados en postgres-central bajo el schema 'crypto'.

Importa get_engine() y ensure_schema() desde shared_etl.db,
que vive en el servidor central (portfolio-server/dags/shared_etl/).
Airflow agrega dags/ al PYTHONPATH, así que shared_etl es importable
desde cualquier submódulo sin configuración extra.
"""

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from shared_etl.db import get_engine, ensure_schema

SCHEMA = "crypto"
TABLE  = "crypto_prices"


def load_data(df: pd.DataFrame) -> None:
    """
    Inserta el DataFrame limpio en crypto.crypto_prices.

    Args:
        df: DataFrame producido por transform_data()
    """
    if df.empty:
        print("DataFrame vacío, nada que cargar.")
        return

    engine = get_engine()

    try:
        ensure_schema(engine, SCHEMA)

        df.to_sql(
            name=TABLE,
            schema=SCHEMA,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        print(f"Cargados {len(df)} registros en {SCHEMA}.{TABLE}.")

    except SQLAlchemyError as e:
        print(f"Error al insertar: {e}")
        raise
    finally:
        engine.dispose()
