# Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# streaming live table
# loans uploader here in every few minutes
@dlt.table(
  schema="""
    transaction_id string,
    payment_date date,
    user_id string,
    ticker string,
    product_category string,
    product_name string,
    merchant_name string,
    product_amount double,
    transaction_fee double,
    cashback double,
    loyalty_points integer,
    payment_method string,
    transaction_status string,
    merchant_id string,
    device_type string,
    location string,
    _rescued_data string
  """,
  table_properties={'quality': 'bronze'})
def raw_trans_18():
  return (
     spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      # .option("cloudFiles.inferColumnTypes", "False")
      .option("header", "true")
      .option("overwriteSchema", "true")
      .option("cloudFiles.schemaHints", "transaction_id string, payment_date date, user_id string, ticker string, product_category string, product_name string, merchant_name string, product_amount double, transaction_fee double, cashback double, loyalty_points integer, payment_method string, transaction_status string, merchant_id string, device_type string, location string")
      .option("skipChangeCommits", "true")
      .load('s3://nft-wallet/Digital Wallet Transactions/')
 )



# Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# materialized views
# reference table, mostly static
@dlt.table(table_properties={'quality': 'bronze'})
def raw_bitcoin_tweet_5():
  return (
     spark.readStream.format('cloudFiles')
     .option('cloudFiles.format', 'csv')
     .option("mode", "PERMISSIVE")
     .option("header", "true")
     .option("inferSchema", "true")
     .schema("user_name STRING, user_location STRING, user_description STRING, user_created TIMESTAMP, user_followers INT, user_friends INT, user_favourites DOUBLE, user_verified Boolean, date TIMESTAMP, text STRING, hashtags STRING, source STRING, is_retweet BOOLEAN")
     .option("skipChangeCommits", "true")
     .load("s3://nft-wallet/Bitcoin Tweets Dataset/")
 )



# Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# materialized views
# reference table, mostly static
@dlt.table(table_properties={'quality': 'bronze'})
def raw_blockchain_tweet_5():
  return (
     spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
     .option("mode", "PERMISSIVE")
     .option("header", "true")
     .schema("user_name STRING, user_location STRING, user_description STRING, user_created TIMESTAMP, user_followers INT, user_friends INT, user_favourites DOUBLE, user_verified Boolean, date TIMESTAMP, text STRING, hashtags STRING, source STRING, is_retweet BOOLEAN")
     .option("skipChangeCommits", "true")
     .load("s3://nft-wallet/Blockchain Tweets/")
 )



# Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# materialized views
# loan from legacy system, new data added every week
@dlt.table(table_properties={'quality': 'bronze'})
def raw_daily_prices():
  return (
     spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      .option("cloudFiles.inferColumnTypes", "true")
      .option("overwriteSchema", "true")
      .option("skipChangeCommits", "true")
      .load('s3://nft-wallet/Crypto currencies daily prices/')
 )



# Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# materialized views
# reference table, mostly static
@dlt.table(table_properties={'quality': 'bronze'})
def cryptocurrencies_symbol_3():
  raw = spark.read.format("json").option("multiline", True).option("skipChangeCommits", "true").load("s3://nft-wallet/Cryptocurrencies Symbols/Cryptocurrencies Symbols.json").collect()[0]
  raw_keys = list(raw.asDict().keys())
  raw_values = [value.lower() for value in list(raw.asDict().values())]
  df = spark.createDataFrame(list(zip(raw_keys, raw_values)), ["symbol", "name"])
  return df





# Silver layer: joining tables while ensuring data quality
# Create structured stream from streaming table
# Keep only the proper transactions. Fail if cost center isn't correct, discard the others.
@dlt.table(
    table_properties={'quality': 'silver'},
    comment="Livestream of new transactions, cleaned and compliant"
)
@dlt.expect("Payments should be this year", "(CAST(payment_date as TIMESTAMP) > date('2020-12-31'))")
@dlt.expect_or_drop("product amount and trans fee should be positive", "(CAST(product_amount as INT) > 0 AND CAST(transaction_fee as INT) > 0)")
@dlt.expect_or_drop("transaction is successfully completed", "(transaction_status in ('Successful'))")
@dlt.expect_or_drop("product category must be specified", "(product_category IS NOT NULL)")
@dlt.expect_or_drop("trans id must be specified", "(transaction_id IS NOT NULL)")
@dlt.expect_or_drop("ticker must be specified", "(ticker IS NOT NULL)")
def cleaned_txs_18():
  txs = dlt.read_stream("raw_trans_18").select("transaction_id", "user_id", "payment_date", "ticker", "product_category", "product_amount", "transaction_fee", "loyalty_points", "location", "transaction_status").alias("txs")
  symbol = dlt.read("cryptocurrencies_symbol_3").alias("symbol")

  return (
    txs.join(symbol, txs.ticker==symbol.symbol, "inner")
     .selectExpr("txs.*", "symbol.name as name")
  )



# Silver layer: joining tables while ensuring data quality
# Create materialized view from table
# Keep only the proper twitters. Fail if cost center isn't correct, discard the others.
@dlt.view(
    comment="CSV of new twitters, cleaned and compliant"
)
def twitters_9():
  blockchain_twitter = dlt.read_stream("raw_blockchain_tweet_5").select("user_name", "user_location", "user_followers", "user_verified", "date", "text", lower(col("hashtags")).alias("hashtags")).alias("blockchain_twitter")

  bitcoin_twitter = dlt.read_stream("raw_bitcoin_tweet_5").select("user_name", "user_location", "user_followers", "user_verified", "date", "text", lower(col("hashtags")).alias("hashtags")).alias("bitcoin_twitter")

  twitter = blockchain_twitter.unionByName(bitcoin_twitter).alias("twitter")

  symbol = dlt.read("cryptocurrencies_symbol_3").alias("symbol")
  return (
    twitter.join(symbol, twitter.hashtags.isNotNull() & twitter.hashtags.contains(symbol.name), "inner")
     .selectExpr("twitter.*", "symbol.symbol as symbol")
 )





# gold layer: joining tables while ensuring data quality
# Create Delta Live Tables from csv
# Keep only the proper twitters. Fail if cost center isn't correct, discard the others.
@dlt.table(table_properties={'quality': 'silver'})
@dlt.expect_or_drop("user name must be specified", "(user_name IS NOT NULL)")
@dlt.expect_or_drop("user location must be specified", "(user_location IS NOT NULL)")
@dlt.expect_or_drop("date must be specified", "(date IS NOT NULL)")
@dlt.expect_or_drop("user followers must be over 20", "(user_followers > 20)")
@dlt.expect_or_drop("hashtags must be specified", "(hashtags IS NOT NULL)")
def cleaned_twitters_9():
  return dlt.read_stream("twitters_9")



# gold layer: joining tables while ensuring data quality
# these tables will be requested at scale using a SQL Endpoint
@dlt.create_table(
  comment="Combines transactions by ticker to generate summary report",
  table_properties={'pipelines.autoOptimize.zOrderCols': 'transaction_fee'}
)
def trans_sum():
  return (
    dlt.read("cleaned_txs_18").groupBy("name", "ticker")
     .agg(
        sum("transaction_fee").alias("transaction_fee"),
        sum("product_amount").alias("product_amount"),
        sum("loyalty_points").alias("loyalty_points")
     )
  )
