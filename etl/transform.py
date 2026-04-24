import pandas as pd

# Standardizes API response keys to internal naming conventions
COLUMNS_MAP = {
    "id": "coin_id",
    "symbol": "symbol",
    "name": "name",
    "current_price": "price_usd",
    "market_cap": "market_cap_usd",
    "total_volume": "volume_24h",
    "price_change_percentage_24h": "change_pct_24h",
    "last_updated": "last_updated_api"
}

def transform_data(raw_data):
    """
    Transforms raw cryptocurrency data into a cleaned DataFrame.
    
    Args:
        raw_data (list): Raw JSON response from the API.
        
    Returns:
        pd.DataFrame: Cleaned data with correct types and timestamps.
    """
    if not raw_data:
        print("No data to transform.")
        return pd.DataFrame()  # Return an empty DataFrame if no data is available
    
    # Initialize DataFrame
    df = pd.DataFrame(raw_data)

    # Select and rename columns based on the mapping
    df = df[list(COLUMNS_MAP.keys())]
    df = df.rename(columns=COLUMNS_MAP)

    # Convert API timestamp to datetime objects
    df["last_updated_api"] = pd.to_datetime(df["last_updated_api"])

    # Ensure data integrity by removing rows missing essential values
    df = df.dropna(subset=["coin_id", "price_usd", "market_cap_usd"])

    # Audit column: record when the data was extracted (UTC)
    df["extracted_at"] = pd.Timestamp.now(tz="UTC")

    return df