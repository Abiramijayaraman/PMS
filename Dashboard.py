
import os
import html

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ==========================================================
# 1. CONFIGURATION
# ==========================================================

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

SCHEMA = "fintel_gold"

# ==========================================================
# 2. CUSTOM STYLING
# ==========================================================

st.markdown(
    """
    <style>
    .stApp {
        background-color: #0b1120;
        color: #e5e7eb;
    }

    [data-testid="stHeader"] {
        background: rgba(11, 17, 32, 0);
    }

    [data-testid="stMetric"] {
        background-color: #151e30;
        border: 1px solid #26334a;
        padding: 18px;
        border-radius: 12px;
    }

    [data-testid="stMetricLabel"] {
        color: #a8b4c8;
    }

    [data-testid="stMetricValue"] {
        color: #f8fafc;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #26334a;
        border-radius: 10px;
    }

    .section-title {
        font-size: 19px;
        font-weight: 650;
        color: #f1f5f9;
        margin-top: 8px;
        margin-bottom: 12px;
    }

    .news-card {
        background-color: #151e30;
        border: 1px solid #26334a;
        border-radius: 12px;
        padding: 14px;
        margin-bottom: 10px;
    }

    .news-headline {
        font-size: 14px;
        font-weight: 600;
        color: #e5e7eb;
    }

    .news-meta {
        font-size: 12px;
        color: #94a3b8;
        margin-top: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ==========================================================
# 3. POSTGRESQL CONNECTION
# ==========================================================

def get_connection():
    """Create a PostgreSQL connection."""

    if not DB_CONFIG["database"] or not DB_CONFIG["password"]:
        raise ValueError(
            "Set PG_DATABASE and PG_PASSWORD in your .env file."
        )

    return psycopg2.connect(**DB_CONFIG)


def fetch_data(query, params=None):
    """Execute SQL and return a DataFrame."""

    conn = get_connection()

    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


# ==========================================================
# 4. LOAD GOLD TABLES
# ==========================================================

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


# ==========================================================
# 5. HELPER FUNCTIONS
# ==========================================================

def format_number(value, decimals=2):
    if pd.isna(value):
        return "—"
    return f"{value:,.{decimals}f}"


def format_percentage(value):
    if pd.isna(value):
        return "—"
    return f"{value:+.2f}%"


def format_timestamp(value):
    if pd.isna(value):
        return "—"

    try:
        return pd.to_datetime(value).strftime("%d %b %Y, %H:%M:%S")
    except Exception:
        return str(value)


def latest_per_symbol(df):
    """Return the most recent window per stock."""

    if df.empty:
        return df

    return (
        df.sort_values("window_start", ascending=False)
        .drop_duplicates(subset=["symbol"], keep="first")
        .reset_index(drop=True)
    )


# ==========================================================
# 6. LOAD DATA
# ==========================================================

try:
    stock_df = load_stock_metrics()
    sentiment_df = load_sentiment_price()
    news_df = load_news()
    alerts_df = load_alerts()

except Exception as e:
    st.error("Unable to connect to PostgreSQL or load Gold data.")
    st.code(str(e))
    st.info(
        "Check PostgreSQL status, database credentials, "
        "schema names and table names."
    )
    st.stop()


# ==========================================================
# 7. HEADER
# ==========================================================

header_col1, header_col2 = st.columns([5, 1])

with header_col1:
    st.markdown(
        """
        <h1 style="margin-bottom:0; color:#f8fafc;">
            📈 FinPulse
        </h1>
        <p style="color:#94a3b8; margin-top:0;">
            Financial Intelligence & Real-Time Market Sentiment
        </p>
        """,
        unsafe_allow_html=True,
    )

with header_col2:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ==========================================================
# 8. FILTERS
# ==========================================================

all_symbols = sorted(
    set(stock_df["symbol"].dropna().tolist())
    | set(sentiment_df["symbol"].dropna().tolist())
    | set(news_df["symbol"].dropna().tolist())
    | set(alerts_df["symbol"].dropna().tolist())
)

filter_col1, filter_col2 = st.columns(2)

with filter_col1:
    selected_symbol = st.selectbox(
        "Select Stock",
        ["ALL"] + all_symbols,
    )

with filter_col2:
    chart_window_option = st.selectbox(
        "Chart History",
        ["20 windows", "50 windows", "100 windows", "All windows"],
        index=1,
    )

chart_limits = {
    "20 windows": 20,
    "50 windows": 50,
    "100 windows": 100,
    "All windows": None,
}

chart_limit = chart_limits[chart_window_option]

# Apply stock filters

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

# ==========================================================
# 9. KPI CALCULATIONS
# ==========================================================

# Count unique articles from individual Gold news records.
total_articles = filtered_news["article_id"].nunique()

# Observed unique symbols across Gold data.
tracked_stocks = len(all_symbols)

# Average latest sentiment window per symbol.
latest_sentiment = latest_per_symbol(filtered_sentiment)

if not latest_sentiment.empty:
    avg_market_sentiment = latest_sentiment["avg_sentiment"].mean()
else:
    avg_market_sentiment = None

# Alert records in Gold.
alert_record_count = len(filtered_alerts)

# Last news processing timestamp.
if (
    not filtered_news.empty
    and filtered_news["processed_at"].notna().any()
):
    last_news_update = filtered_news["processed_at"].max()
else:
    last_news_update = None

# Last market metrics timestamp.
if (
    not filtered_stock.empty
    and filtered_stock["computed_at"].notna().any()
):
    last_market_update = filtered_stock["computed_at"].max()
else:
    last_market_update = None

# ==========================================================
# 10. MARKET OVERVIEW KPI CARDS
# ==========================================================

st.markdown(
    '<div class="section-title">Market Overview</div>',
    unsafe_allow_html=True,
)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

kpi1.metric(
    "📰 News Articles",
    f"{total_articles:,}",
)

kpi2.metric(
    "📊 Stocks Tracked",
    f"{tracked_stocks:,}",
)

kpi3.metric(
    "🧠 Average Sentiment",
    format_number(avg_market_sentiment, 3)
    if avg_market_sentiment is not None
    else "—",
)

kpi4.metric(
    "🚨 Alert Records",
    f"{alert_record_count:,}",
)

# Data freshness

fresh_col1, fresh_col2 = st.columns(2)

with fresh_col1:
    st.caption(
        f"📰 Last News Update: {format_timestamp(last_news_update)}"
    )

with fresh_col2:
    st.caption(
        f"📈 Last Market Update: {format_timestamp(last_market_update)}"
    )

st.divider()

# ==========================================================
# 11. STOCK SNAPSHOT
# ==========================================================

st.markdown(
    '<div class="section-title">📊 Stock Snapshot</div>',
    unsafe_allow_html=True,
)

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
    st.info("No stock metrics available.")


# ==========================================================
# 12. SENTIMENT BY STOCK
# ==========================================================

st.markdown(
    '<div class="section-title">🧠 Sentiment by Stock</div>',
    unsafe_allow_html=True,
)

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
    st.info("No sentiment data available.")

st.divider()

# ==========================================================
# 13. SENTIMENT VS PRICE MOVEMENT
# ==========================================================

st.markdown(
    '<div class="section-title">📉 Sentiment vs Price Movement</div>',
    unsafe_allow_html=True,
)

if not filtered_sentiment.empty:

    chart_symbols = sorted(
        filtered_sentiment["symbol"].dropna().unique()
    )

    if selected_symbol == "ALL":
        chart_symbol = st.selectbox(
            "Choose stock for historical analysis",
            chart_symbols,
            key="chart_symbol",
        )
    else:
        chart_symbol = selected_symbol

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

        # Sentiment series
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
                    "Sentiment: %{y:.3f}<extra></extra>"
                ),
            )
        )

        # Price change series
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
                    "Price Change: %{y:.2f}%<extra></extra>"
                ),
            )
        )

        fig.update_layout(
            height=420,
            template="plotly_dark",
            paper_bgcolor="#0b1120",
            plot_bgcolor="#151e30",
            margin=dict(l=10, r=10, t=35, b=10),
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(
                title="Window Start",
                gridcolor="#26334a",
            ),
            yaxis=dict(
                title="Sentiment Score",
                range=[-1, 1],
                gridcolor="#26334a",
                zeroline=True,
            ),
            yaxis2=dict(
                title="Price Change (%)",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No historical chart data available.")

else:
    st.info("No sentiment-price data available.")

st.divider()

# ==========================================================
# 14. LATEST FINANCIAL NEWS
# ==========================================================

st.markdown(
    '<div class="section-title">📰 Latest Financial News</div>',
    unsafe_allow_html=True,
)

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

        # Escape external text before rendering HTML.
        headline = html.escape(
            str(row.get("headline") or "No headline available")
        )

        symbol = html.escape(
            str(row.get("symbol") or "N/A")
        )

        source = html.escape(
            str(row.get("source") or "Unknown source")
        )

        sentiment = html.escape(
            str(row.get("sentiment") or "Unknown")
        )

        score = row.get("sentiment_score")
        score_text = format_number(score, 3)
        published = format_timestamp(row.get("published_at"))

        st.markdown(
            f"""
            <div class="news-card">
                <div class="news-headline">{headline}</div>

                <div class="news-meta">
                    <b>{symbol}</b> &nbsp;|&nbsp;
                    {source} &nbsp;|&nbsp;
                    {published}
                </div>

                <div class="news-meta">
                    Sentiment: <b>{sentiment.upper()}</b>
                    &nbsp;|&nbsp;
                    Score: <b>{score_text}</b>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

else:
    st.info("No news records available.")


st.divider()

# ==========================================================
# 15. MARKET ALERTS
# ==========================================================

st.markdown(
    '<div class="section-title">🚨 Market Alerts</div>',
    unsafe_allow_html=True,
)

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
    st.success("No alert records available for this selection.")

# ==========================================================
# 16. FOOTER
# ==========================================================

st.markdown(
    """
    <div style="text-align:center; color:#64748b; padding:20px;">
        FinPulse Financial Intelligence Dashboard
        <br>
        Powered by Kafka • PySpark • PostgreSQL • Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)
