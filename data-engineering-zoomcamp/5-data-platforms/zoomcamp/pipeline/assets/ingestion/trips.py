"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

@bruin"""

import os
import json
from datetime import datetime
import pandas as pd
import requests


def materialize():
    """
    Fetch NYC taxi trip data from TLC public dataset.

    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE for date range
    - BRUIN_VARS for configuration (taxi_types)

    Returns a DataFrame with normalized taxi trip records.
    """
    # Get date window from Bruin environment
    start_date = os.getenv("BRUIN_START_DATE", "2024-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2024-01-31")

    # Get pipeline variables (taxi types to fetch)
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    config = json.loads(bruin_vars) if isinstance(bruin_vars, str) else bruin_vars
    taxi_types = config.get("taxi_types", ["yellow", "green"])

    # Current timestamp for lineage tracking
    extracted_at = datetime.utcnow()

    dataframes = []

    # Fetch data for each taxi type
    for taxi_type in taxi_types:
        try:
            df = _fetch_taxi_data(taxi_type, start_date, end_date, extracted_at)
            if df is not None and not df.empty:
                dataframes.append(df)
        except Exception as e:
            print(f"Warning: Failed to fetch {taxi_type} taxi data: {str(e)}")
            continue

    # Combine all data
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
    else:
        # Return empty DataFrame with correct schema if no data fetched
        final_df = _create_empty_dataframe(extracted_at)

    # Remove timezone information from datetime columns to avoid PyArrow timezone database issues
    for col in ["pickup_datetime", "dropoff_datetime", "extracted_at"]:
        if col in final_df.columns and pd.api.types.is_datetime64_any_dtype(
            final_df[col]
        ):
            # Convert to UTC if timezone-aware, then remove timezone
            if final_df[col].dt.tz is not None:
                final_df[col] = final_df[col].dt.tz_convert("UTC").dt.tz_localize(None)

    print(f"Fetched {len(final_df)} records for taxi trip data.")
    print(final_df.head(5))
    return final_df


def _fetch_taxi_data(
    taxi_type: str, start_date: str, end_date: str, extracted_at: datetime
) -> pd.DataFrame:
    """
    Fetch taxi trip data from NYC TLC public dataset.

    The TLC provides parquet files accessible via URLs:
    https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet
    """
    try:
        # Parse dates
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        # Generate list of months to fetch
        date_range = pd.date_range(start=start, end=end, freq="MS")

        all_dfs = []

        for date in date_range:
            year = date.year
            month = date.month

            # Construct TLC data URL
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

            try:
                # Fetch and read parquet file
                df = pd.read_parquet(url)

                # Normalize column names to lowercase
                df.columns = df.columns.str.lower()

                # Select and rename relevant columns based on taxi type
                df = _normalize_columns(df, taxi_type)

                # Filter to requested date range
                df = df[
                    (df["pickup_datetime"] >= start) & (df["pickup_datetime"] <= end)
                ]

                all_dfs.append(df)

            except Exception as e:
                print(
                    f"Warning: Could not fetch {taxi_type} data for {year}-{month:02d}: {str(e)}"
                )
                continue

        if all_dfs:
            result_df = pd.concat(all_dfs, ignore_index=True)
            result_df["extracted_at"] = extracted_at

            # Remove timezone info from datetime columns
            for col in ["pickup_datetime", "dropoff_datetime"]:
                if col in result_df.columns and pd.api.types.is_datetime64_any_dtype(
                    result_df[col]
                ):
                    if result_df[col].dt.tz is not None:
                        result_df[col] = (
                            result_df[col].dt.tz_convert("UTC").dt.tz_localize(None)
                        )

            return result_df
        else:
            return _create_empty_dataframe(extracted_at)

    except Exception as e:
        print(f"Error fetching {taxi_type} taxi data: {str(e)}")
        return _create_empty_dataframe(extracted_at)


def _normalize_columns(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """
    Normalize TLC data columns to standard schema.
    Yellow and green taxi files have slightly different column names.
    """
    # Map of potential column names for each field
    column_mapping = {
        "vendor_id": ["vendorid", "vendor_id"],
        "pickup_datetime": [
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "pickup_datetime",
        ],
        "dropoff_datetime": [
            "tpep_dropoff_datetime",
            "lpep_dropoff_datetime",
            "dropoff_datetime",
        ],
        "passenger_count": ["passenger_count"],
        "trip_distance": ["trip_distance"],
        "fare_amount": ["fare_amount"],
        "tip_amount": ["tip_amount"],
        "total_amount": ["total_amount"],
        "payment_type": ["payment_type"],
    }

    # Find and rename columns
    selected_cols = {}
    for target_col, possible_names in column_mapping.items():
        for possible_name in possible_names:
            if possible_name in df.columns:
                selected_cols[possible_name] = target_col
                break

    # Select and rename columns
    df = df[[col for col in selected_cols.keys()]].rename(columns=selected_cols)

    # Generate trip_id from existing data
    df["trip_id"] = (
        df.index.astype(str) + "_" + pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    )

    # Rename payment_type to payment_type_id
    if "payment_type" in df.columns:
        df = df.rename(columns={"payment_type": "payment_type_id"})

    # # Ensure datetime columns are properly typed and timezone-naive
    # for col in ["pickup_datetime", "dropoff_datetime"]:
    #     if col in df.columns:
    #         df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
    #         # Strip timezone if present to avoid PyArrow issues on Windows
    #         if df[col].dt.tz is not None:
    #             df[col] = df[col].dt.tz_localize(None)

    return df[
        [
            "trip_id",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type_id",
        ]
    ]


def _create_empty_dataframe(extracted_at: datetime) -> pd.DataFrame:
    """
    Create an empty DataFrame with the correct schema.
    """
    return pd.DataFrame(
        {
            "trip_id": pd.Series([], dtype="object"),
            "vendor_id": pd.Series([], dtype="int64"),
            "pickup_datetime": pd.Series([], dtype="datetime64[ns]"),
            "dropoff_datetime": pd.Series([], dtype="datetime64[ns]"),
            "passenger_count": pd.Series([], dtype="int64"),
            "trip_distance": pd.Series([], dtype="float64"),
            "fare_amount": pd.Series([], dtype="float64"),
            "tip_amount": pd.Series([], dtype="float64"),
            "total_amount": pd.Series([], dtype="float64"),
            "payment_type_id": pd.Series([], dtype="int64"),
            "extracted_at": pd.Series([], dtype="datetime64[ns]"),
        }
    )
