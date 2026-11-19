(.venv) PS C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\src> python run_silver_pipeline.py    
                         
================================================
       STARTING SILVER NEWS PIPELINE SERVICE                 
================================================
Bronze folder: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\news_bronze\shared_data\bronze\new_news_update
Mode: CONTINUOUS
WARNING: Using incubator modules: jdk.incubator.vector                 
26/11/19 14:42:41 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset. -see https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/11/19 14:42:43 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

========== SERVICE MODEL INITIALIZATION ==========

Loading local semantic embedding model...
Loading weights: 100%|█████████████████████████████████████████████████████████| 103/103 [00:00<00:00, 1392.81it/s]
Embedding model loaded successfully.

========== LOADING FINBERT ==========
Using device: cpu
Tokenizer loaded successfully.
Loading weights: 100%|██████████████████████████████████████████████████████████| 201/201 [00:00<00:00, 323.73it/s]
FinBERT model loaded successfully!

Model sentiment labels:
{'positive': 0, 'negative': 1, 'neutral': 2}
Model initialization completed in 2.07 seconds

========== CHECKPOINT RECOVERY ==========
Committed batches recovered: 6
Processed Bronze files: 20
Processed article IDs: 47
Checkpoint recovery completed.

Service is running. Monitoring Bronze for new Parquet files.
Press Ctrl+C to stop the service gracefully.


Detected 1 new Bronze file(s). Checking file stability...


================================================
          STARTING SILVER NEWS BATCH
================================================

========== STEP 1: CHECKPOINT RECOVERY ==========

========== CHECKPOINT RECOVERY ==========
Committed batches recovered: 6
Processed Bronze files: 20
Processed article IDs: 47
Checkpoint recovery completed.

========== STEP 2: READ BRONZE ==========

========== INCREMENTAL FILE CHECK ==========
Total Bronze files: 4
Previously processed files: 20
New files to process: 1
NEW: news_20261119T065518393195Z_2af22eaa.parquet
SKIPPED: news_20261118T062018498715Z_0678ba20.parquet
SKIPPED: news_20261118T062018665286Z_11170060.parquet
SKIPPED: news_20261119T065518299142Z_85c25b66.parquet

Reading: news_20261119T065518393195Z_2af22eaa.parquet
Records in file: 2
                                                                                
Total new Bronze records: 2
New Bronze files: 1
 - news_20261119T065518393195Z_2af22eaa.parquet

Batch ID: batch_4633011e44fac97b

========== STEP 3: EXACT DEDUPLICATION ==========

========== ARTICLE-LEVEL DEDUPLICATION ==========
Previously processed article IDs: 47
                                                                                
========== DEDUPLICATION SUMMARY ==========
Incoming records: 2
Previously processed IDs: 47
Records with missing article IDs: 0
New unique articles: 2

Article-level deduplication completed!
Records proceeding to validation: 2                                             
                                                                                
========== STEP 4: VALIDATION ==========

========== STARTING NEWS VALIDATION ==========
                                                                                
========== VALIDATION SUMMARY ==========
Total candidate records: 2
Valid records: 2
Invalid records: 0

Validation completed!
Valid articles: 2                                                               
Invalid articles: 0

========== STEP 5: HTML CLEANING ==========

========== STARTING HTML CLEANING ==========
HTML cleaning completed.

========== STEP 6: TICKER EXTRACTION ==========

========== STARTING TICKER EXTRACTION ==========
Number of companies loaded: 13
                                                                                
========== TICKER EXTRACTION RESULTS ==========
Total articles: 2
Articles with tickers: 2
Articles without tickers: 0
Total article-ticker mappings: 2                                                

ARTICLE-LEVEL TICKER RESULTS:
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+
|article_id                                                      |cleaned_headline                                                                                                                                                        |matched_tickers|
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+
|04416cd594c941718009f41fab2b69ab2b215f0647a2ba22cff8c80da8310639|ICICI Bank Hits a 52-Week High: Can Growth Drivers Sustain the Rally? - TradingView                                                                                     |[ICICIBANK]    |
|2a922252f8120bf5931c3d38d54ed2e95370d84866c3e9598faa2088b9056c30|Cyril Amarchand Mangaldas advises Bajaj Finance Ltd. as its legal counsel as to Indian law on its INR 11700 crore QIP and INR 5800 crore preferential issue - SCC Online|[BAJFINANCE]   |
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+


NEWS BY TICKER RESULTS:
+----------------------------------------------------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|article_id                                                      |ticker    |cleaned_headline                                                                                                                                                        |
+----------------------------------------------------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|04416cd594c941718009f41fab2b69ab2b215f0647a2ba22cff8c80da8310639|ICICIBANK |ICICI Bank Hits a 52-Week High: Can Growth Drivers Sustain the Rally? - TradingView                                                                                     |
|2a922252f8120bf5931c3d38d54ed2e95370d84866c3e9598faa2088b9056c30|BAJFINANCE|Cyril Amarchand Mangaldas advises Bajaj Finance Ltd. as its legal counsel as to Indian law on its INR 11700 crore QIP and INR 5800 crore preferential issue - SCC Online|
+----------------------------------------------------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


TICKER EXTRACTION COMPLETED!

========== STEP 7: SEMANTIC DEDUPLICATION ==========

========== SEMANTIC DEDUPLICATION ==========
                                                                                
Loading historical Silver News...
Reading historical file: part-00000.parquet
Reading historical file: part-00000.parquet
Reading historical file: part-00000.parquet
Reading historical file: part-00000.parquet
Reading historical file: part-00000.parquet
Reading historical file: part-00000.parquet
Historical articles loaded: 46

Preparing semantic comparison candidates...

UNIQUE ARTICLE: 04416cd594c941718009f41fab2b69ab2b215f0647a2ba22cff8c80da8310639

UNIQUE ARTICLE: 2a922252f8120bf5931c3d38d54ed2e95370d84866c3e9598faa2088b9056c30

========== SEMANTIC DEDUPLICATION SUMMARY ==========
Incoming articles: 2
Historical articles: 46
Semantic comparisons performed: 0
Semantic duplicates: 0
Unique incoming articles: 2
Similarity threshold: 0.9
Time window: 24 hours

Semantic deduplication completed!
Articles after semantic deduplication: 2                                        
Semantic duplicates detected: 0
Unique article-ticker mappings: 2                                               

========== STEP 8: FINBERT SENTIMENT ==========

========== STARTING FINBERT SENTIMENT ANALYSIS ==========
Total unique articles: 2                                                        
FinBERT already loaded; reusing the in-memory model.

Starting FinBERT inference...
Processed 2/2 articles
                                                                                
========== ARTICLE-LEVEL SENTIMENT ==========
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------+------------------+--------------------+------------------+------------------+
|article_id                                                      |cleaned_headline                                                                                                                                                        |matched_tickers|sentiment|positive_score    |negative_score      |neutral_score     |sentiment_score   |
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------+------------------+--------------------+------------------+------------------+
|04416cd594c941718009f41fab2b69ab2b215f0647a2ba22cff8c80da8310639|ICICI Bank Hits a 52-Week High: Can Growth Drivers Sustain the Rally? - TradingView                                                                                     |[ICICIBANK]    |positive |0.5798910856246948|0.0171740110963583  |0.4029349088668823|0.5627170745283365|
|2a922252f8120bf5931c3d38d54ed2e95370d84866c3e9598faa2088b9056c30|Cyril Amarchand Mangaldas advises Bajaj Finance Ltd. as its legal counsel as to Indian law on its INR 11700 crore QIP and INR 5800 crore preferential issue - SCC Online|[BAJFINANCE]   |neutral  |0.0377463772892952|0.021727070212364197|0.94052654504776  |0.016019307076931 |
+----------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------+------------------+--------------------+------------------+------------------+


========== TICKER-LEVEL SENTIMENT ==========
+----------------------------------------------------------------+----------+---------+------------------+
|article_id                                                      |ticker    |sentiment|sentiment_score   |
+----------------------------------------------------------------+----------+---------+------------------+
|04416cd594c941718009f41fab2b69ab2b215f0647a2ba22cff8c80da8310639|ICICIBANK |positive |0.5627170745283365|
|2a922252f8120bf5931c3d38d54ed2e95370d84866c3e9598faa2088b9056c30|BAJFINANCE|neutral  |0.016019307076931 |
+----------------------------------------------------------------+----------+---------+------------------+


========================================
FINBERT SENTIMENT ANALYSIS COMPLETED
========================================
Articles processed: 2
Article-ticker records: 2

Sentiment analysis completed successfully.

========== STEP 9: WRITE OUTPUTS + AUDIT ==========

========== SAFE BATCH WRITING ==========

Writing article-level Silver data...

Writing Parquet using PyArrow...
Records to write: 2                                                             
Parquet file written: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\_staging\batch_4633011e44fac97b\news\part-00000.parquet

Writing ticker-level Silver data...

Writing Parquet using PyArrow...
Records to write: 2                                                             
Parquet file written: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\_staging\batch_4633011e44fac97b\news_by_ticker\part-00000.parquet

Writing DLQ records...

Writing Parquet using PyArrow...
Records to write: 0                                                             
Parquet file written: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\_staging\batch_4633011e44fac97b\dead_letter\part-00000.parquet

Writing semantic duplicate audit...

Writing Parquet using PyArrow...
Records to write: 0                                                             
Parquet file written: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\_staging\batch_4633011e44fac97b\semantic_duplicates\part-00000.parquet
Commit marker created: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\checkpoints\committed_batches\batch_4633011e44fac97b.json

========================================
BATCH COMMITTED SUCCESSFULLY
========================================
Batch ID: batch_4633011e44fac97b
Bronze files: 1
Article IDs: 2
News output: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\silver\news\batch_id=batch_4633011e44fac97b
Ticker output: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\silver\news_by_ticker\batch_id=batch_4633011e44fac97b
DLQ output: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\processed\dead_letter\batch_id=batch_4633011e44fac97b
Semantic duplicate audit: C:\Users\2000209448\Documents\FinTel_FINAL_With_Cosine\silver_news\data\audit\semantic_duplicates\batch_id=batch_4633011e44fac97b

========== STEP 10: UPDATE CHECKPOINTS ==========
Historical cache updated with committed articles: 2                             
Checkpoint files updated successfully.

================================================
          SILVER NEWS BATCH COMPLETED
================================================
Batch ID: batch_4633011e44fac97b
Bronze files processed: 1
Articles considered: 2
Valid articles: 2
Invalid articles: 0
Semantic duplicates: 0
Unique articles: 2
Article IDs checkpointed: 2

========== EXECUTION TIME SUMMARY ==========
Checkpoint Recovery           :      0.04 seconds
Read Bronze                   :     42.03 seconds
Exact Deduplication           :     69.23 seconds
Validation                    :    112.83 seconds
HTML Cleaning                 :      0.29 seconds
Ticker Extraction             :    191.73 seconds
Semantic Deduplication        :     34.76 seconds
FinBERT Inference             :    137.25 seconds
Write Silver + DLQ + Audit    :     91.78 seconds
Update Historical Cache       :     17.77 seconds
TOTAL BATCH TIME              :    964.11 seconds
============================================
