import hashlib
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# Windows PySpark must use the active virtual-environment interpreter.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lit

from read_bronze import read_new_bronze, BRONZE_PATH
from deduplicate_news import (
    deduplicate_news,
    semantic_deduplicate_news,
    load_embedding_model,
    register_committed_articles,
)
from validate_news import validate_news
from clean_news import clean_news
from extract_tickers import extract_tickers
from sentiment_analysis import analyse_sentiment, load_finbert
from checkpoint_manager import (
    load_processed_files,
    save_processed_files,
    load_processed_article_ids,
    save_processed_article_ids,
)
from safe_output_writer import (
    write_batch_outputs,
    create_commit_marker,
    recover_checkpoints,
)

POLL_INTERVAL_SECONDS = 5
FILE_STABILITY_CHECKS = 2


def generate_batch_id(bronze_files):
    filenames = sorted(Path(file).name for file in bronze_files)
    file_string = "|".join(filenames)
    batch_hash = hashlib.sha256(file_string.encode("utf-8")).hexdigest()[:16]
    return f"batch_{batch_hash}"


def get_article_ids(news_df):
    if news_df is None:
        return set()
    rows = (
        news_df.select("article_id")
        .where(col("article_id").isNotNull() & (trim(col("article_id")) != ""))
        .distinct()
        .collect()
    )
    return {row["article_id"] for row in rows}


def add_empty_sentiment_columns(dataframe):
    sentiment_columns = {
        "sentiment": "string",
        "positive_score": "double",
        "negative_score": "double",
        "neutral_score": "double",
        "sentiment_score": "double",
    }
    for column_name, data_type in sentiment_columns.items():
        if column_name not in dataframe.columns:
            dataframe = dataframe.withColumn(column_name, lit(None).cast(data_type))
    return dataframe


class StageTimer:
    """Collect per-stage wall-clock durations for terminal reporting only."""
    def __init__(self):
        self.durations = []
        self.started_at = time.perf_counter()

    def run(self, label, function, *args, **kwargs):
        start = time.perf_counter()
        try:
            return function(*args, **kwargs)
        finally:
            self.durations.append((label, time.perf_counter() - start))

    def print_summary(self):
        print("\n========== EXECUTION TIME SUMMARY ==========")
        for label, seconds in self.durations:
            print(f"{label:<30}: {seconds:>9.2f} seconds")
        print(f"{'TOTAL BATCH TIME':<30}: {time.perf_counter() - self.started_at:>9.2f} seconds")
        print("============================================")


def process_one_batch(spark):
    """Process all currently available, stable Bronze files using an existing Spark session."""
    timer = StageTimer()
    batch_started = datetime.now(timezone.utc)
    print("\n\n================================================")
    print("          STARTING SILVER NEWS BATCH")
    print("================================================")

    try:
        print("\n========== STEP 1: CHECKPOINT RECOVERY ==========")
        timer.run("Checkpoint Recovery", recover_checkpoints)

        print("\n========== STEP 2: READ BRONZE ==========")
        news_df, new_files = timer.run("Read Bronze", read_new_bronze, spark)
        if not new_files:
            print("\nNo new Bronze files found. Waiting for the next file.")
            return False

        bronze_filenames = sorted(Path(file).name for file in new_files)
        print("New Bronze files:", len(bronze_filenames))
        for filename in bronze_filenames:
            print(" -", filename)
        batch_id = generate_batch_id(bronze_filenames)
        print("\nBatch ID:", batch_id)

        if news_df is None:
            print("\nNew files contain no records. Committing empty batch.")
            create_commit_marker(batch_id, bronze_filenames, [])
            timer.run("Checkpoint Recovery", recover_checkpoints)
            return True

        print("\n========== STEP 3: EXACT DEDUPLICATION ==========")
        unique_new_df, missing_id_df = timer.run("Exact Deduplication", deduplicate_news, news_df)
        candidates_df = unique_new_df.unionByName(missing_id_df, allowMissingColumns=True)
        candidate_count = timer.run("Candidate count action", candidates_df.count)
        print("Records proceeding to validation:", candidate_count)
        batch_article_ids = timer.run("Collect article IDs", get_article_ids, candidates_df)

        print("\n========== STEP 4: VALIDATION ==========")
        valid_df, invalid_df = timer.run("Validation", validate_news, candidates_df)
        valid_count = timer.run("Valid record count action", valid_df.count)
        invalid_count = timer.run("Invalid record count action", invalid_df.count)
        print("Valid articles:", valid_count)
        print("Invalid articles:", invalid_count)

        print("\n========== STEP 5: HTML CLEANING ==========")
        cleaned_df = timer.run("HTML Cleaning", clean_news, valid_df)

        print("\n========== STEP 6: TICKER EXTRACTION ==========")
        news_with_tickers_df, news_by_ticker_df = timer.run("Ticker Extraction", extract_tickers, cleaned_df)

        print("\n========== STEP 7: SEMANTIC DEDUPLICATION ==========")
        unique_news_df, semantic_duplicates_df = timer.run(
            "Semantic Deduplication", semantic_deduplicate_news, news_with_tickers_df
        )
        # Add batch metadata to every audit row before the atomic batch write.
        semantic_duplicates_df = (
            semantic_duplicates_df
            .withColumn("batch_id", lit(batch_id))
            .withColumn("detected_at_utc", lit(batch_started.isoformat()))
        )
        unique_count = timer.run("Unique article count action", unique_news_df.count)
        duplicate_count = timer.run("Duplicate audit count action", semantic_duplicates_df.count)
        print("Articles after semantic deduplication:", unique_count)
        print("Semantic duplicates detected:", duplicate_count)

        unique_article_ids = unique_news_df.select("article_id").distinct()
        unique_news_by_ticker_df = news_by_ticker_df.join(unique_article_ids, on="article_id", how="inner")
        ticker_mapping_count = timer.run(
            "Ticker mapping count action",
            unique_news_by_ticker_df.count
        )
        print("Unique article-ticker mappings:", ticker_mapping_count)

        print("\n========== STEP 8: FINBERT SENTIMENT ==========")
        if unique_count > 0:
            news_with_sentiment_df, news_by_ticker_with_sentiment_df = timer.run(
                "FinBERT Inference", analyse_sentiment, unique_news_df, unique_news_by_ticker_df
            )
        else:
            print("No unique articles. Skipping FinBERT.")
            news_with_sentiment_df = add_empty_sentiment_columns(unique_news_df)
            news_by_ticker_with_sentiment_df = add_empty_sentiment_columns(unique_news_by_ticker_df)

        print("\n========== STEP 9: WRITE OUTPUTS + AUDIT ==========")
        committed = timer.run(
            "Write Silver + DLQ + Audit",
            write_batch_outputs,
            batch_id=batch_id,
            news_with_sentiment_df=news_with_sentiment_df,
            news_by_ticker_with_sentiment_df=news_by_ticker_with_sentiment_df,
            invalid_df=invalid_df,
            semantic_duplicates_df=semantic_duplicates_df,
            bronze_files=bronze_filenames,
            processed_article_ids=batch_article_ids,
            semantic_duplicate_count=duplicate_count,
        )

        print("\n========== STEP 10: UPDATE CHECKPOINTS ==========")
        if committed:
            # Keep the in-memory history aligned with disk only after the output commit succeeds.
            timer.run("Update Historical Cache", register_committed_articles, unique_news_df)
            processed_files = load_processed_files()
            processed_files.update(bronze_filenames)
            save_processed_files(processed_files)
            processed_ids = load_processed_article_ids()
            processed_ids.update(batch_article_ids)
            save_processed_article_ids(processed_ids)
            print("Checkpoint files updated successfully.")
        else:
            print("Batch already committed; recovering checkpoints.")
            recover_checkpoints()

        print("\n================================================")
        print("          SILVER NEWS BATCH COMPLETED")
        print("================================================")
        print("Batch ID:", batch_id)
        print("Bronze files processed:", len(bronze_filenames))
        print("Articles considered:", candidate_count)
        print("Valid articles:", valid_count)
        print("Invalid articles:", invalid_count)
        print("Semantic duplicates:", duplicate_count)
        print("Unique articles:", unique_count)
        print("Article IDs checkpointed:", len(batch_article_ids))
        timer.print_summary()
        return True

    except Exception as error:
        print("\n================================================")
        print("          SILVER NEWS BATCH FAILED")
        print("================================================")
        print("Error:", str(error))
        timer.print_summary()
        raise


def wait_until_files_are_stable(files):
    """Wait until each new Parquet file has an unchanged size across checks."""
    previous_sizes = {}
    stable_counts = {Path(file): 0 for file in files}
    pending = set(Path(file) for file in files)
    while pending:
        for file in list(pending):
            try:
                current_size = file.stat().st_size
            except FileNotFoundError:
                pending.remove(file)
                continue
            if previous_sizes.get(file) == current_size and current_size > 0:
                stable_counts[file] += 1
            else:
                stable_counts[file] = 0
            previous_sizes[file] = current_size
            if stable_counts[file] >= FILE_STABILITY_CHECKS:
                pending.remove(file)
        if pending:
            time.sleep(1)


def run_silver_pipeline(continuous=True):
    print("\n================================================")
    print("       STARTING SILVER NEWS PIPELINE SERVICE")
    print("================================================")
    print("Bronze folder:", BRONZE_PATH)
    print("Mode:", "CONTINUOUS" if continuous else "ONE-TIME")

    spark = (
        SparkSession.builder
        .appName("SilverNewsPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Load both ML models once at service startup, then reuse them for all batches.
        startup_start = time.perf_counter()
        print("\n========== SERVICE MODEL INITIALIZATION ==========")
        load_embedding_model()
        load_finbert()
        print(f"Model initialization completed in {time.perf_counter() - startup_start:.2f} seconds")

        recover_checkpoints()

        if not continuous:
            process_one_batch(spark)
            return

        print("\nService is running. Monitoring Bronze for new Parquet files.")
        print("Press Ctrl+C to stop the service gracefully.\n")
        while True:
            pending_files = [Path(p) for p in sorted(BRONZE_PATH.glob("*.parquet"))
                             if p.name not in load_processed_files()]
            if pending_files:
                print(f"\nDetected {len(pending_files)} new Bronze file(s). Checking file stability...")
                wait_until_files_are_stable(pending_files)
                try:
                    process_one_batch(spark)
                except Exception as batch_error:
                    print("\nBatch failed; files remain uncheckpointed and will be retried.")
                    print("Retrying after 15 seconds. Error:", batch_error)
                    time.sleep(15)
            else:
                time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nCtrl+C received. Shutting down Silver service...")
    finally:
        spark.stop()
        print("Spark session stopped. Silver service shut down.")


if __name__ == "__main__":
    # Continuous mode is the default. Use --once for a single batch and exit.
    run_silver_pipeline(continuous="--once" not in sys.argv)