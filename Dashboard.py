import os
import html

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ============================================================
# 1. CONFIGURATION
# ============================================================

load_dotenv()

st.set_page_config(
    page_title="FinPulse | Financial Intelligence",
    page_icon="📈",
    layout="wide",
)

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", ""),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

# Actual Gold schema
SCHEMA = "fintel_gold"

# ============================================================
# 2. LIGHT THEME CSS
# ============================================================

st.markdown(
    """
    <style>
    /* Entire application */
    .stApp {
        background-color: #f5f7fb;
        color: #172033;
    }

    [data-testid="stHeader"] {
        background-color: #f5f7fb;
    }

    [data-testid="stToolbar"] {
        background-color: transparent;
    }

    /* Main content */
    [data-testid="stMainBlockContainer"] {
        padding-top: 1.5rem;
    }

    /* Headings */
    h1, h2, h3, h4 {
        color: #14233b !important;
    }

    /* KPI metric cards */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e3e9f2;
        padding: 18px 20px;
        border-radius: 14px;
        box-shadow: 0 2px 8px rgba(25, 45, 75, 0.04);
    }

    [data-testid="stMetricLabel"] {
        color: #64748b !important;
    }

    [data-testid="stMetricValue"] {
        color: #172033 !important;
    }

    [data-testid="stMetricDelta"] {
        font-weight: 600;
    }

    /* Tables */
    [data-testid="stDataFrame"] {
        background-color: #ffffff;
        border: 1px solid #e3e9f2;
        border-radius: 12px;
    }

    /* Select boxes */
    [data-testid="stSelectbox"] {
        background-color: #ffffff;
    }

    /* News cards */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff;
        border-color: #e3e9f2 !important;
        border-radius: 12px;
    }

    /* Buttons */
    .stButton > button {
        border-radius: 9px;
        border: 1px solid #d9e2ef;
        background-color: #ffffff;
        color: #1d3557;
        font-weight: 600;
    }

    .stButton > button:hover {
        border-color: #3b82f6;
        color: #2563eb;
    }

    /* Dividers */
    hr {
        border-color: #e3e9f2 !important;
    }

    /* Captions */
    [data-testid="stCaptionContainer"] {
        color: #64748b;
    }

    /* Alerts */
    [data-testid="stAlert"] {
        border-radius: 10px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# 3. POSTGRESQL CONNECTION
# ============================================================

def get_connection():
    """Create a PostgreSQL connection."""

    if not DB_CONFIG["database"]:
        raise ValueError("PG_DATABASE is missing from your .env file.")

    if not DB_CONFIG["password"]:
        raise ValueError("PG_PASSWORD is missing from your .env file.")

    return psycopg2.connect(**DB_CONFIG)


def fetch_data(query, params=None):
    """Execute SQL query and return the result as a DataFrame."""

    conn = get_connection()

    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


# ============================================================
# 4. LOAD GOLD TABLES
# ============================================================

@st.cache_data(ttl=15)
def load_stock_metrics():
    query = """
        SELECT *
        FROM fintel_gold.gold_stock_metrics
        ORDER BY window_start DESC
    """
    return fetch_data(query)


@st.cache_data(ttl=15)
def load_sentiment_price():
    query = """
        SELECT *
        FROM fintel_gold.gold_sentiment_price
        ORDER BY window_start DESC
    """
    return fetch_data(query)


@st.cache_data(ttl=15)
def load_news():
    query = """
        SELECT
            article_id,
            symbol,
            headline,
            source,
            published_at,
            sentiment,
            sentiment_score,
            positive_score,
            negative_score,
            neutral_score,
            processed_at
        FROM fintel_gold.gold_news_sentiment
        ORDER BY published_at DESC NULLS LAST
    """
    return fetch_data(query)


@st.cache_data(ttl=15)
def load_alerts():
    query = """
        SELECT
            alert_id,
            symbol,
            window_start,
            alert_type,
            alert_message,
            sentiment_score,
            price_change_pct,
            created_at
        FROM fintel_gold.gold_alerts
        ORDER BY created_at DESC
    """
    return fetch_data(query)


# ============================================================
# 5. HELPER FUNCTIONS
# ============================================================

def format_number(value, decimals=2):
    if value is None or pd.isna(value):
        return "—"

    try:
        return f"{float(value):,.{decimals}f}"
    except (ValueError, TypeError):
        return "—"


def format_percentage(value):
    if value is None or pd.isna(value):
        return "—"

    try:
        return f"{float(value):+.2f}%"
    except (ValueError, TypeError):
        return "—"


def format_timestamp(value):
    if value is None or pd.isna(value):
        return "—"

    try:
        return pd.to_datetime(value).strftime("%d %b %Y, %H:%M:%S")
    except Exception:
        return str(value)


def latest_per_symbol(df):
    """Return latest window for each symbol."""

    if df.empty:
        return df

    return (
        df.sort_values("window_start", ascending=False)
        .drop_duplicates(subset=["symbol"], keep="first")
        .reset_index(drop=True)
    )


def safe_value(value, default="—"):
    if value is None or pd.isna(value):
        return default
    return str(value)


# ============================================================
# 6. LOAD DATA
# ============================================================

try:
    stock_df = load_stock_metrics()
    sentiment_df = load_sentiment_price()
    news_df = load_news()
    alerts_df = load_alerts()

except Exception as e:
    st.error("Unable to connect to PostgreSQL or load Gold data.")

    st.warning(
        "Please check your PostgreSQL service, .env credentials, "
        "database name, schema, and table names."
    )

    with st.expander("View technical error"):
        st.code(str(e))

    st.stop()


# ============================================================
# 7. HEADER
# ============================================================

header_left, header_right = st.columns([5, 1])

with header_left:
    st.title("📈 FinPulse")
    st.caption("Financial Intelligence & Real-Time Market Sentiment")

with header_right:
    st.write("")
    st.write("")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()


# ============================================================
# 8. STOCK FILTERS
# ============================================================

all_symbols = sorted(
    set(stock_df["symbol"].dropna().tolist())
    | set(sentiment_df["symbol"].dropna().tolist())
    | set(news_df["symbol"].dropna().tolist())
    | set(alerts_df["symbol"].dropna().tolist())
)

filter_col1, filter_col2 = st.columns([1, 1])

with filter_col1:
    selected_symbol = st.selectbox(
        "Stock Symbol",
        options=["ALL"] + all_symbols,
    )

with filter_col2:
    chart_window_option = st.selectbox(
        "Chart History",
        options=[
            "20 windows",
            "50 windows",
            "100 windows",
            "All windows",
        ],
        index=1,
    )

chart_limits = {
    "20 windows": 20,
    "50 windows": 50,
    "100 windows": 100,
    "All windows": None,
}

chart_limit = chart_limits[chart_window_option]


# Apply symbol filtering

if selected_symbol == "ALL":

    filtered_stock = stock_df.copy()
    filtered_sentiment = sentiment_df.copy()
    filtered_news = news_df.copy()
    filtered_alerts = alerts_df.copy()

else:

    filtered_stock = stock_df[
        stock_df["symbol"] == selected_symbol
    ].copy()

    filtered_sentiment = sentiment_df[
        sentiment_df["symbol"] == selected_symbol
    ].copy()

    filtered_news = news_df[
        news_df["symbol"] == selected_symbol
    ].copy()

    filtered_alerts = alerts_df[
        alerts_df["symbol"] == selected_symbol
    ].copy()


# ============================================================
# 9. KPI CALCULATIONS
# ============================================================

# Unique news article count from individual Gold news records.
total_articles = filtered_news["article_id"].nunique()

# Observed unique symbols from Gold.
# This is not the configured watchlist count.
tracked_stocks = len(all_symbols)

# Average latest sentiment per symbol.
latest_sentiment = latest_per_symbol(filtered_sentiment)

if (
    not latest_sentiment.empty
    and latest_sentiment["avg_sentiment"].notna().any()
):
    avg_market_sentiment = latest_sentiment["avg_sentiment"].mean()
else:
    avg_market_sentiment = None

# Alert records from Gold alerts table.
alert_record_count = len(filtered_alerts)

# Last news processing timestamp.
if (
    not filtered_news.empty
    and filtered_news["processed_at"].notna().any()
):
    last_news_update = filtered_news["processed_at"].max()
else:
    last_news_update = None

# Last stock metric calculation timestamp.
if (
    not filtered_stock.empty
    and filtered_stock["computed_at"].notna().any()
):
    last_market_update = filtered_stock["computed_at"].max()
else:
    last_market_update = None


# ============================================================
# 10. MARKET OVERVIEW
# ============================================================

st.subheader("Market Overview")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric(
        label="📰 News Articles",
        value=f"{total_articles:,}",
    )

with kpi2:
    st.metric(
        label="📊 Stocks Tracked",
        value=f"{tracked_stocks:,}",
    )

with kpi3:
    st.metric(
        label="🧠 Average Sentiment",
        value=(
            format_number(avg_market_sentiment, 3)
            if avg_market_sentiment is not None
            else "—"
        ),
    )

with kpi4:
    st.metric(
        label="🚨 Alert Records",
        value=f"{alert_record_count:,}",
    )


# Data freshness
freshness_col1, freshness_col2 = st.columns(2)

with freshness_col1:
    st.caption(
        f"📰 Last News Update: {format_timestamp(last_news_update)}"
    )

with freshness_col2:
    st.caption(
        f"📈 Last Market Update: {format_timestamp(last_market_update)}"
    )

st.divider()


# ============================================================
# 11. STOCK SNAPSHOT
# ============================================================

st.subheader("📊 Stock Snapshot")

latest_stock = latest_per_symbol(filtered_stock)

if not latest_stock.empty:

    stock_display = latest_stock[
        [
            "symbol",
            "window_start",
            "price_last",
            "price_change_pct",
            "day_change_pct",
            "price_range_pct",
        ]
    ].copy()

    stock_display.rename(
        columns={
            "symbol": "Symbol",
            "window_start": "Latest Window",
            "price_last": "Last Price",
            "price_change_pct": "Window Change %",
            "day_change_pct": "Day Change %",
            "price_range_pct": "Price Range %",
        },
        inplace=True,
    )

    stock_display["Last Price"] = stock_display["Last Price"].round(2)
    stock_display["Window Change %"] = stock_display["Window Change %"].round(2)
    stock_display["Day Change %"] = stock_display["Day Change %"].round(2)
    stock_display["Price Range %"] = stock_display["Price Range %"].round(2)

    st.dataframe(
        stock_display,
        use_container_width=True,
        hide_index=True,
    )

else:
    st.info("No stock metrics are available for this selection.")


# ============================================================
# 12. SENTIMENT BY STOCK
# ============================================================

st.subheader("🧠 Sentiment by Stock")

latest_sentiment_table = latest_per_symbol(filtered_sentiment)

if not latest_sentiment_table.empty:

    sentiment_display = latest_sentiment_table[
        [
            "symbol",
            "avg_sentiment",
            "min_sentiment",
            "latest_sentiment_label",
            "article_count",
            "price_change_pct",
            "alert_flag",
        ]
    ].copy()

    sentiment_display.rename(
        columns={
            "symbol": "Symbol",
            "avg_sentiment": "Average Sentiment",
            "min_sentiment": "Minimum Sentiment",
            "latest_sentiment_label": "Latest News Sentiment",
            "article_count": "Articles",
            "price_change_pct": "Price Change %",
            "alert_flag": "Alert",
        },
        inplace=True,
    )

    sentiment_display["Average Sentiment"] = (
        sentiment_display["Average Sentiment"].round(3)
    )

    sentiment_display["Minimum Sentiment"] = (
        sentiment_display["Minimum Sentiment"].round(3)
    )

    sentiment_display["Price Change %"] = (
        sentiment_display["Price Change %"].round(2)
    )

    st.dataframe(
        sentiment_display,
        use_container_width=True,
        hide_index=True,
    )

else:
    st.info("No sentiment records are available.")


st.divider()


# ============================================================
# 13. SENTIMENT VS PRICE MOVEMENT
# ============================================================

st.subheader("📉 Sentiment vs Price Movement")

if not filtered_sentiment.empty:

    chart_symbols = sorted(
        filtered_sentiment["symbol"].dropna().unique()
    )

    if selected_symbol == "ALL" and chart_symbols:

        chart_symbol = st.selectbox(
            "Select stock for chart",
            chart_symbols,
            key="chart_symbol",
        )

    elif chart_symbols:
        chart_symbol = selected_symbol

    else:
        chart_symbol = None

    if chart_symbol:

        chart_df = filtered_sentiment[
            filtered_sentiment["symbol"] == chart_symbol
        ].copy()

        chart_df["window_start"] = pd.to_datetime(
            chart_df["window_start"]
        )

        chart_df = chart_df.sort_values("window_start")

        if chart_limit is not None:
            chart_df = chart_df.tail(chart_limit)

        if not chart_df.empty:

            fig = go.Figure()

            # Sentiment line
            fig.add_trace(
                go.Scatter(
                    x=chart_df["window_start"],
                    y=chart_df["avg_sentiment"],
                    name="Sentiment Score",
                    mode="lines+markers",
                    line=dict(width=2),
                    yaxis="y",
                    hovertemplate=(
                        "Time: %{x}<br>"
                        "Sentiment: %{y:.3f}"
                        "<extra></extra>"
                    ),
                )
            )

            # Price movement line
            fig.add_trace(
                go.Scatter(
                    x=chart_df["window_start"],
                    y=chart_df["price_change_pct"],
                    name="Price Change %",
                    mode="lines",
                    line=dict(width=2, dash="dot"),
                    yaxis="y2",
                    hovertemplate=(
                        "Time: %{x}<br>"
                        "Price Change: %{y:.2f}%"
                        "<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                height=420,
                template="plotly_white",
                paper_bgcolor="#ffffff",
                plot_bgcolor="#ffffff",
                font=dict(
                    color="#172033",
                ),
                margin=dict(
                    l=15,
                    r=15,
                    t=40,
                    b=15,
                ),
                legend=dict(
                    orientation="h",
                    y=1.12,
                ),
                xaxis=dict(
                    title="Window Start",
                    gridcolor="#e8edf4",
                ),
                yaxis=dict(
                    title="Sentiment Score",
                    range=[-1, 1],
                    gridcolor="#e8edf4",
                    zeroline=True,
                    zerolinecolor="#cbd5e1",
                ),
                yaxis2=dict(
                    title="Price Change (%)",
                    overlaying="y",
                    side="right",
                    showgrid=False,
                ),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

        else:
            st.info("No historical chart data is available.")

else:
    st.info("No sentiment-price data is available.")


st.divider()


# ============================================================
# 14. LATEST FINANCIAL NEWS
# ============================================================

st.subheader("📰 Latest Financial News")

if not filtered_news.empty:

    latest_news_display = (
        filtered_news.sort_values(
            "published_at",
            ascending=False,
            na_position="last",
        )
        .drop_duplicates(
            subset=["article_id", "symbol"]
        )
        .head(10)
    )

    for _, row in latest_news_display.iterrows():

        # Convert null values to readable text.
        headline = safe_value(
            row.get("headline"),
            "No headline available",
        )

        symbol = safe_value(
            row.get("symbol"),
            "N/A",
        )

        source = safe_value(
            row.get("source"),
            "Unknown source",
        )

        sentiment = safe_value(
            row.get("sentiment"),
            "Unknown",
        )

        score = row.get("sentiment_score")
        score_text = format_number(score, 3)
        published = format_timestamp(row.get("published_at"))

        # Native Streamlit rendering: no raw HTML.
        with st.container(border=True):

            st.text(headline)

            st.caption(
                f"{symbol}  •  {source}  •  {published}"
            )

            st.caption(
                f"Sentiment: {sentiment.upper()}  •  Score: {score_text}"
            )

else:
    st.info("No news records are available.")


st.divider()


# ============================================================
# 15. MARKET ALERTS
# ============================================================

st.subheader("🚨 Market Alerts")

if not filtered_alerts.empty:

    alerts_display = filtered_alerts[
        [
            "created_at",
            "symbol",
            "alert_type",
            "alert_message",
            "sentiment_score",
            "price_change_pct",
        ]
    ].copy()

    alerts_display.rename(
        columns={
            "created_at": "Created At",
            "symbol": "Symbol",
            "alert_type": "Alert Type",
            "alert_message": "Alert Message",
            "sentiment_score": "Sentiment Score",
            "price_change_pct": "Price Change %",
        },
        inplace=True,
    )

    alerts_display["Sentiment Score"] = (
        alerts_display["Sentiment Score"].round(3)
    )

    alerts_display["Price Change %"] = (
        alerts_display["Price Change %"].round(2)
    )

    st.dataframe(
        alerts_display.head(20),
        use_container_width=True,
        hide_index=True,
    )

else:
    st.success("No alert records for the selected stock.")


# ============================================================
# 16. FOOTER
# ============================================================

st.divider()

st.caption(
    "FinPulse Financial Intelligence Dashboard"
    "  |  Kafka • PySpark • PostgreSQL • Streamlit"
)
