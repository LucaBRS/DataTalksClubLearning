import pandas as pd
import os

# ── Output directories ───────────────────────────────────────────────────────
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ── Data sources ─────────────────────────────────────────────────────────────
# Each entry: (file_name, url, final_column_name)
SOURCES = [
    (
        "marriage_rate",
        "https://ourworldindata.org/grapher/marriage-rate-per-1000-inhabitants.csv?v=1&csvType=full&useColumnShortNames=true",
        "marriage_rate",
    ),
    (
        "divorce_rate",
        "https://ourworldindata.org/grapher/divorces-per-1000-people.csv?v=1&csvType=full&useColumnShortNames=true",
        "divorce_rate",
    ),
    (
        "age_at_marriage_women",
        "https://ourworldindata.org/grapher/age-at-marriage-women.csv?v=1&csvType=full&useColumnShortNames=true",
        "age_at_marriage_women",
    ),
    (
        "schooling_boys",
        "https://ourworldindata.org/grapher/years-of-schooling.csv?v=1&csvType=full&useColumnShortNames=true&level=all&metric_type=average_years_schooling&sex=boys",
        "schooling_years_boys",
    ),
    (
        "schooling_girls",
        "https://ourworldindata.org/grapher/years-of-schooling.csv?v=1&csvType=full&useColumnShortNames=true&level=all&metric_type=average_years_schooling&sex=girls",
        "schooling_years_girls",
    ),
    (
        "gdp_per_capita",
        "https://ourworldindata.org/grapher/gdp-per-capita-worldbank.csv?v=1&csvType=full&useColumnShortNames=true",
        "gdp_per_capita",
    ),
    (
        "gender_inequality",
        "https://ourworldindata.org/grapher/gender-inequality-index-from-the-human-development-report.csv?v=1&csvType=full&useColumnShortNames=true",
        "gender_inequality_index",
    ),
    (
        "happiness",
        "https://ourworldindata.org/grapher/happiness-cantril-ladder.csv?v=1&csvType=full&useColumnShortNames=true",
        "happiness_score",
    ),
]


def download_and_clean(name: str, url: str, value_col: str) -> pd.DataFrame:
    """Download a CSV, rename the value column, and save the raw file."""
    print(f"Downloading: {name}...")
    df = pd.read_csv(url)

    # Save raw CSV
    df.to_csv(f"{RAW_DIR}/{name}.csv", index=False)

    # Rename the value column (always the last one in Our World in Data datasets)
    value_original = df.columns[-1]
    df = df.rename(columns={value_original: value_col})

    # Drop rows with no value
    df = df.dropna(subset=[value_col])

    # Keep only relevant columns
    cols_to_keep = ["entity", "code", "year", value_col]
    if "owid_region" in df.columns:
        cols_to_keep.append("owid_region")
    df = df[cols_to_keep]

    print(f"  → {len(df)} rows, years: {df['year'].min()}–{df['year'].max()}")
    return df


def merge_all(dataframes: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """Merge all DataFrames on entity + year (outer join)."""
    merged = dataframes[0][1]

    for name, df in dataframes[1:]:
        cols = [c for c in df.columns if c not in ["code", "owid_region"]]
        merged = merged.merge(df[cols], on=["entity", "year"], how="outer")

    return merged


def main():
    dataframes = []

    for name, url, value_col in SOURCES:
        df = download_and_clean(name, url, value_col)
        dataframes.append((name, df))

    print("\nMerging all datasets...")
    final = merge_all(dataframes)

    final = final.rename(columns={"entity": "country", "code": "country_code"})

    print(f"Final dataset: {final.shape[0]} rows, {final.shape[1]} columns")
    print(f"Columns: {list(final.columns)}")
    print(f"Countries: {final['country'].nunique()}")
    print(f"Years: {final['year'].min()}–{final['year'].max()}")

    output_path = f"{PROCESSED_DIR}/relationships_dataset.parquet"
    final.to_parquet(output_path, index=False)
    print(f"\nSaved to: {output_path}")

    print("\nPreview:")
    print(final.head())


if __name__ == "__main__":
    main()
