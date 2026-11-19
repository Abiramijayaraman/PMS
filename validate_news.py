
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    concat_ws
)


# --------------------------------------------------
# 1. NEWS VALIDATION
# --------------------------------------------------

def validate_news(candidates_df):

    print("\n========== STARTING NEWS VALIDATION ==========")

    # Required field checks.
    has_article_id = (
        col("article_id").isNotNull()
        & (trim(col("article_id")) != "")
    )

    has_url = (
        col("url").isNotNull()
        & (trim(col("url")) != "")
    )

    has_headline = (
        col("headline").isNotNull()
        & (trim(col("headline")) != "")
    )

    has_full_text = (
        col("full_text").isNotNull()
        & (trim(col("full_text")) != "")
    )

    has_content = has_headline | has_full_text

    # Generate validation reasons.
    validation_reason = concat_ws(
        "; ",
        when(
            col("json_parse_failed") == True,
            lit("invalid_json")
        ),
        when(
            ~has_article_id,
            lit("missing_article_id")
        ),
        when(
            ~has_url,
            lit("missing_url")
        ),
        when(
            ~has_content,
            lit("missing_headline_and_full_text")
        )
    )

    validated_df = (
        candidates_df
        .withColumn(
            "validation_reason",
            validation_reason
        )
        .withColumn(
            "validation_status",
            when(
                col("validation_reason") == "",
                lit("VALID")
            ).otherwise(lit("INVALID"))
        )
    )

    # Separate valid and invalid records.
    valid_df = validated_df.filter(
        col("validation_status") == "VALID"
    )

    invalid_df = (
        validated_df
        .filter(col("validation_status") == "INVALID")
        .withColumn(
            "rejected_at",
            lit(None).cast("string")
        )
    )

    # Avoid three separate Spark count() jobs inside validation.
    # The resulting DataFrames are returned directly to the pipeline.
    print("\nValidation transformations completed; valid and invalid records are separated.")

    return valid_df, invalid_df