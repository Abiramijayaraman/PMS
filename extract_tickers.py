
import json
import re
from pathlib import Path

from pyspark.sql.functions import (
    col,
    lit,
    when,
    array,
    filter as array_filter,
    explode,
    size
)


# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ALIAS_PATH = (
    PROJECT_ROOT
    / "silver_news"
    / "config"
    / "company_aliases.json"
)


# --------------------------------------------------
# 2. LOAD COMPANY ALIASES
# --------------------------------------------------

def load_company_aliases():

    if not ALIAS_PATH.exists():
        raise FileNotFoundError(
            f"Alias file not found: {ALIAS_PATH}"
        )

    with open(
        ALIAS_PATH,
        "r",
        encoding="utf-8"
    ) as file:

        company_aliases = json.load(file)

    print(
        "Number of companies loaded:",
        len(company_aliases)
    )

    return company_aliases


# --------------------------------------------------
# 3. EXTRACT TICKERS
# --------------------------------------------------

def extract_tickers(cleaned_df):

    print("\n========== STARTING TICKER EXTRACTION ==========")

    company_aliases = load_company_aliases()

    analysis_text = col("text_for_analysis")

    ticker_conditions = []

    for ticker, aliases in company_aliases.items():

        patterns = []

        for alias in aliases:

            escaped_alias = re.escape(alias)

            pattern = (
                r"(?i)(^|[^A-Za-z0-9])"
                + escaped_alias
                + r"([^A-Za-z0-9]|$)"
            )

            patterns.append(pattern)

        combined_pattern = "|".join(patterns)

        condition = analysis_text.rlike(
            combined_pattern
        )

        ticker_conditions.append(
            when(condition, lit(ticker))
        )

    # Create an array containing matched tickers.
    df_with_tickers = cleaned_df.withColumn(
        "matched_tickers",
        array_filter(
            array(*ticker_conditions),
            lambda ticker: ticker.isNotNull()
        )
    )

    # Avoid count() actions here. They force Spark to execute the
    # ticker-expression plan before semantic deduplication needs it.

    # --------------------------------------------------
    # 5. CREATE NEWS BY TICKER
    # --------------------------------------------------

    news_by_ticker_df = (
        df_with_tickers
        .withColumn(
            "ticker",
            explode(col("matched_tickers"))
        )
    )

    # Avoid .count() and .show() here: semantic deduplication and
    # sentiment processing consume these DataFrames downstream.
    print("Ticker extraction transformations prepared.")

    return df_with_tickers, news_by_ticker_df