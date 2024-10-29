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



