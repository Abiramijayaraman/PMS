
from pathlib import Path
from datetime import datetime, timezone, timedelta
from functools import lru_cache

import pyarrow.parquet as pq

from sentence_transformers import SentenceTransformer, util

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    trim,
    row_number
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from checkpoint_manager import load_processed_article_ids


# ==================================================
# 1. PROJECT PATHS AND CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_NEWS_PATH = (
    PROJECT_ROOT
    / "silver_news"
    / "data"
    / "silver"
    / "news"
)

# Load the embedding model from the local computer.
EMBEDDING_MODEL_NAME = str(
    PROJECT_ROOT / "silver_news" / "models" / "all-MiniLM-L6-v2"
)

SIMILARITY_THRESHOLD = 0.90

TIME_WINDOW_HOURS = 24

# Model is loaded only once and reused.
embedding_model = None
historical_news_cache = None


# ==================================================
# 2. LOAD EMBEDDING MODEL
# ==================================================

def load_embedding_model():

    global embedding_model

    if embedding_model is None:

        print("\nLoading local semantic embedding model...")

        embedding_model = SentenceTransformer(
            EMBEDDING_MODEL_NAME,
            local_files_only=True
        )

        print("Embedding model loaded successfully.")

    return embedding_model


# ==================================================
# 3. ARTICLE-LEVEL DEDUPLICATION
# ==================================================

def deduplicate_news(news_df):

    print("\n========== ARTICLE-LEVEL DEDUPLICATION ==========")

    processed_ids = load_processed_article_ids()

    print(
        "Previously processed article IDs:",
        len(processed_ids)
    )

    # Separate records with missing article IDs.

    missing_id_df = news_df.filter(
        col("article_id").isNull()
        | (trim(col("article_id")) == "")
    )

    # Keep records with article IDs.

    identified_df = news_df.filter(
        col("article_id").isNotNull()
        & (trim(col("article_id")) != "")
    )

    # Remove previously processed articles.

    if processed_ids:

        identified_df = identified_df.filter(
            ~col("article_id").isin(list(processed_ids))
        )

    # Remove duplicates within the current batch.

    window_spec = (
        Window
        .partitionBy("article_id")
        .orderBy(
            col("ingested_at").desc_nulls_last(),
            col("url").asc_nulls_last(),
            col("headline").asc_nulls_last()
        )
    )

    ranked_df = identified_df.withColumn(
        "row_number",
        row_number().over(window_spec)
    )

    unique_new_df = (
        ranked_df
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    # Summary.

    incoming_count = news_df.count()
    missing_count = missing_id_df.count()
    unique_count = unique_new_df.count()

    print("\n========== DEDUPLICATION SUMMARY ==========")

    print("Incoming records:", incoming_count)
    print("Previously processed IDs:", len(processed_ids))
    print("Records with missing article IDs:", missing_count)
    print("New unique articles:", unique_count)

    print("\nArticle-level deduplication completed!")

    return unique_new_df, missing_id_df


# ==================================================
# 4. NORMALIZE TIMESTAMP
# ==================================================

def normalize_timestamp(value):

    if value is None:
        return None

    if isinstance(value, str):

        try:

            value = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

        except ValueError:

            return None

    if not isinstance(value, datetime):
        return None

    if value.tzinfo is None:

        value = value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(timezone.utc)


# ==================================================
# 5. GET ARTICLE TEXT
# ==================================================

def get_article_text(article):

    headline = (
        article.get("cleaned_headline")
        or article.get("headline")
        or ""
    )

    return str(headline).strip()


# ==================================================
# 6. GET ARTICLE TICKERS
# ==================================================

def get_article_tickers(article):

    tickers = article.get("matched_tickers")

    if not tickers:
        return set()

    return {
        str(ticker).strip().upper()
        for ticker in tickers
        if ticker
    }


# ==================================================
# 7. READ PREVIOUSLY COMMITTED SILVER NEWS
# ==================================================

def load_historical_news():

    global historical_news_cache

    if historical_news_cache is not None:
        print("Reusing in-memory historical Silver News cache:", len(historical_news_cache))
        return historical_news_cache

    print("\nLoading historical Silver News...")

    historical_articles = []

    if not SILVER_NEWS_PATH.exists():

        print("Silver News directory does not exist yet.")

        return historical_articles

    parquet_files = list(
        SILVER_NEWS_PATH.rglob("*.parquet")
    )

    if not parquet_files:

        print("No historical Silver News found.")

        return historical_articles

    for parquet_file in parquet_files:

        print(
            "Reading historical file:",
            parquet_file.name
        )

        try:

            table = pq.read_table(parquet_file)

            records = table.to_pylist()

            historical_articles.extend(records)

        except Exception as error:

            raise RuntimeError(
                f"Could not read historical Silver file "
                f"{parquet_file}: {error}"
            ) from error

    print(
        "Historical articles loaded:",
        len(historical_articles)
    )

    historical_news_cache = historical_articles
    return historical_news_cache


def register_committed_articles(news_df):
    """Add successfully committed unique articles to the service's history cache."""
    global historical_news_cache
    if news_df is None:
        return
    new_records = [row.asDict(recursive=True) for row in news_df.collect()]
    if historical_news_cache is None:
        # If semantic comparison has not initialized the cache yet, load disk history first.
        load_historical_news()
    known_ids = {item.get("article_id") for item in historical_news_cache}
    for record in new_records:
        article_id = record.get("article_id")
        if article_id and article_id not in known_ids:
            historical_news_cache.append(record)
            known_ids.add(article_id)
    print("Historical cache updated with committed articles:", len(new_records))


# ==================================================
# 8. CHECK TICKER AND TIME WINDOW
# ==================================================

def is_candidate_article(
    incoming_article,
    historical_article
):

    incoming_tickers = get_article_tickers(
        incoming_article
    )

    historical_tickers = get_article_tickers(
        historical_article
    )

    # Both articles must have at least one common ticker.

    if not incoming_tickers.intersection(
        historical_tickers
    ):

        return False

    incoming_time = normalize_timestamp(
        incoming_article.get("ingested_at")
    )

    historical_time = normalize_timestamp(
        historical_article.get("ingested_at")
    )

    # Do not discard records when timestamps are missing.
    # They cannot be safely compared using the time rule.

    if incoming_time is None or historical_time is None:

        return False

    time_difference = abs(
        incoming_time - historical_time
    )

    allowed_window = timedelta(
        hours=TIME_WINDOW_HOURS
    )

    return time_difference <= allowed_window


# ==================================================
# 9. GENERATE EMBEDDING
# ==================================================

@lru_cache(maxsize=20000)
def generate_embedding(text):
    """Cache normalized embeddings by exact text for reuse across batches."""
    model = load_embedding_model()
    embedding = model.encode(
        text,
        convert_to_tensor=True,
        normalize_embeddings=True
    )
    return embedding


# ==================================================
# 10. CALCULATE COSINE SIMILARITY
# ==================================================

def calculate_similarity(
    incoming_text,
    historical_text
):

    incoming_embedding = generate_embedding(
        incoming_text
    )

    historical_embedding = generate_embedding(
        historical_text
    )

    similarity = util.cos_sim(
        incoming_embedding,
        historical_embedding
    )

    return float(
        similarity.item()
    )


# ==================================================
# 11. SEMANTIC DEDUPLICATION
# ==================================================

def semantic_deduplicate_news(
    news_with_tickers_df
):

    print("\n========== SEMANTIC DEDUPLICATION ==========")

    incoming_articles = [
        row.asDict(recursive=True)
        for row in news_with_tickers_df.collect()
    ]

    if not incoming_articles:

        print("No incoming articles for semantic comparison.")

        empty_duplicate_schema = StructType([
            StructField("duplicate_article_id", StringType(), True),
            StructField("canonical_article_id", StringType(), True),
            StructField("duplicate_source", StringType(), True),
            StructField("canonical_source", StringType(), True),
            StructField("duplicate_headline", StringType(), True),
            StructField("canonical_headline", StringType(), True),
            StructField("matched_tickers", StringType(), True),
            StructField("similarity_score", DoubleType(), True),
            StructField("similarity_threshold", DoubleType(), True),
            StructField("time_window_hours", DoubleType(), True),
            StructField("duplicate_reason", StringType(), True),
            StructField("detected_at_utc", StringType(), True)
        ])

        duplicate_df = news_with_tickers_df.sparkSession.createDataFrame(
            [],
            empty_duplicate_schema
        )

        return news_with_tickers_df, duplicate_df

    # --------------------------------------------------
    # LOAD HISTORICAL ARTICLES
    # --------------------------------------------------

    historical_articles = load_historical_news()

    # --------------------------------------------------
    # PREPARE CANDIDATES
    # --------------------------------------------------

    print("\nPreparing semantic comparison candidates...")

    # Process articles in ingestion-time order.

    incoming_articles.sort(
        key=lambda article: (
            normalize_timestamp(article.get("ingested_at"))
            or datetime.min.replace(tzinfo=timezone.utc)
        )
    )

    # Only unique accepted articles become comparison
    # candidates for later articles in this batch.

    accepted_articles = []

    # Previously committed articles are comparison candidates.

    comparison_articles = list(historical_articles)

    duplicate_records = []

    duplicate_ids = set()

    comparison_count = 0

    # --------------------------------------------------
    # COMPARE EACH INCOMING ARTICLE
    # --------------------------------------------------

    for incoming in incoming_articles:

        article_id = incoming.get("article_id")

        incoming_text = get_article_text(
            incoming
        )

        incoming_tickers = get_article_tickers(
            incoming
        )

        if not incoming_text:

            print(
                "Skipping semantic comparison because "
                "headline is empty:",
                article_id
            )

            accepted_articles.append(incoming)

            comparison_articles.append(incoming)

            continue

        if not incoming_tickers:

            print(
                "No ticker found. Retaining article:",
                article_id
            )

            accepted_articles.append(incoming)

            comparison_articles.append(incoming)

            continue

        best_match = None

        best_similarity = 0.0

        # --------------------------------------------------
        # FIND SAME-TICKER ARTICLES WITHIN 24 HOURS
        # --------------------------------------------------

        for candidate in comparison_articles:

            if candidate.get("article_id") == article_id:
                continue

            if not is_candidate_article(
                incoming,
                candidate
            ):
                continue

            candidate_text = get_article_text(
                candidate
            )

            if not candidate_text:
                continue

            # --------------------------------------------------
            # COSINE SIMILARITY
            # --------------------------------------------------

            similarity = calculate_similarity(
                incoming_text,
                candidate_text
            )

            comparison_count += 1

            print(
                f"Comparing {article_id} with "
                f"{candidate.get('article_id')}: "
                f"{similarity:.4f}"
            )

            if similarity > best_similarity:

                best_similarity = similarity

                best_match = candidate

        # --------------------------------------------------
        # IDENTIFY DUPLICATE
        # --------------------------------------------------

        if (
            best_match is not None
            and best_similarity >= SIMILARITY_THRESHOLD
        ):

            canonical_id = best_match.get(
                "article_id"
            )

            print(
                "\nSEMANTIC DUPLICATE DETECTED:",
                article_id
            )

            print(
                "Canonical article:",
                canonical_id
            )

            print(
                "Similarity:",
                round(best_similarity, 4)
            )

            matched_tickers = sorted(
                incoming_tickers.intersection(get_article_tickers(best_match))
            )
            print("Duplicate headline:", incoming_text)
            print("Canonical headline:", get_article_text(best_match))
            print("Matched ticker(s):", ", ".join(matched_tickers))
            print("Reason: shared ticker, within 24 hours, similarity >=", SIMILARITY_THRESHOLD)

            duplicate_ids.add(article_id)

            duplicate_records.append({

                "duplicate_article_id": article_id,

                "canonical_article_id": canonical_id,

                "duplicate_source": incoming.get("source"),

                "canonical_source": best_match.get("source"),

                "duplicate_headline": incoming_text,

                "canonical_headline": get_article_text(
                    best_match
                ),

                "matched_tickers": ", ".join(
                    sorted(
                        incoming_tickers.intersection(
                            get_article_tickers(best_match)
                        )
                    )
                ),

                "similarity_score": best_similarity,
                "similarity_threshold": SIMILARITY_THRESHOLD,
                "time_window_hours": TIME_WINDOW_HOURS,
                "duplicate_reason": "Same ticker within 24-hour window and cosine similarity at or above threshold",
                "detected_at_utc": datetime.now(timezone.utc).isoformat()
            })

        else:

            print(
                "\nUNIQUE ARTICLE:",
                article_id
            )

            accepted_articles.append(incoming)

            comparison_articles.append(incoming)

    # --------------------------------------------------
    # CREATE UNIQUE SPARK DATAFRAME
    # --------------------------------------------------

    if duplicate_ids:

        unique_df = news_with_tickers_df.filter(
            ~col("article_id").isin(
                list(duplicate_ids)
            )
        )

    else:

        unique_df = news_with_tickers_df

    # --------------------------------------------------
    # CREATE DUPLICATE AUDIT DATAFRAME
    # --------------------------------------------------

    duplicate_schema = StructType([

        StructField("duplicate_article_id", StringType(), True),

        StructField("canonical_article_id", StringType(), True),

        StructField("duplicate_source", StringType(), True),

        StructField("canonical_source", StringType(), True),

        StructField("duplicate_headline", StringType(), True),

        StructField("canonical_headline", StringType(), True),

        StructField("matched_tickers", StringType(), True),

        StructField("similarity_score", DoubleType(), True),

        StructField("similarity_threshold", DoubleType(), True),

        StructField("time_window_hours", DoubleType(), True),

        StructField("duplicate_reason", StringType(), True),

        StructField("detected_at_utc", StringType(), True)

    ])

    duplicate_df = (
        news_with_tickers_df.sparkSession.createDataFrame(
            duplicate_records,
            schema=duplicate_schema
        )
    )

    # --------------------------------------------------
    # SUMMARY
    # --------------------------------------------------

    print("\n========== SEMANTIC DEDUPLICATION SUMMARY ==========")

    print(
        "Incoming articles:",
        len(incoming_articles)
    )

    print(
        "Historical articles:",
        len(historical_articles)
    )

    print(
        "Semantic comparisons performed:",
        comparison_count
    )

    print(
        "Semantic duplicates:",
        len(duplicate_records)
    )

    print(
        "Unique incoming articles:",
        len(incoming_articles) - len(duplicate_records)
    )

    print(
        "Similarity threshold:",
        SIMILARITY_THRESHOLD
    )

    print(
        "Time window:",
        TIME_WINDOW_HOURS,
        "hours"
    )

    print("\nSemantic deduplication completed!")

    return unique_df, duplicate_df