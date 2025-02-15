--- SOLUTION : SQL
WITH CTE AS (SELECT 
  user_id,
  count('tweet_id') as tweet_bucket
FROM tweets
WHERE tweet_date >= '01/01/2022' AND tweet_date <= '12/31/2022'
GROUP BY user_id)
SELECT
  tweet_bucket,
  count(user_id) AS users_num
FROM CTE 
GROUP BY tweet_bucket


---SOLUTION : KQL
-- let CTE = tweets
-- | where tweet_date >= datetime('2022-01-01') and tweet_date <= datetime('2022-12-31')
-- | summarize tweet_bucket = count(tweet_id) by user_id;
-- CTE
-- | summarize users_num = count(user_id) by tweet_bucket



---SOLUTION - PYSPARK
-- from pyspark.sql import SparkSession
-- from pyspark.sql.functions import col,count
-- from pyspark.sql.types import StructType,StructField,IntegerType,StringType

-- spark = SparkSession.builder.appName("Histogram of Tweets").getOrCreate()

-- # Step 2: Define the Schema using StructType and StructField
-- schema = StructType([
--     StructField("tweet_id", IntegerType(), False),
--     StructField("user_id", IntegerType(), False),
--     StructField("msg", StringType(), False),
--     StructField("tweet_date", StringType(), False)  # Keeping as String; can be converted to Timestamp later
-- ])

-- # Step 3: Create Data (List of Tuples)
-- data = [
--     (214252, 111, "Am considering taking Tesla private at $420. Funding secured.", "2021-12-30 00:00:00"),
--     (739252, 111, "Despite the constant negative press covfefe", "2022-01-01 00:00:00"),
--     (846402, 111, "Following @NickSinghTech on Twitter changed my life!", "2022-02-14 00:00:00"),
--     (241425, 254, "If the salary is so competitive why won’t you tell me what it is?", "2022-03-01 00:00:00"),
--     (231574, 148, "I no longer have a manager. I can't be managed", "2022-03-23 00:00:00")
-- ]

-- tweets_df = spark.createDataFrame(data,schema)

-- filter_df = tweets_df.where(tweets_df['tweet_date'] >= '01/01/2022' and tweets_df['tweet_date'] <= '12/31/2022')
-- grouped_df = filter_df.group by(filter_df['user_id']).agg(count(filter_df['tweet_id').alias('tweet_bucket'))
-- result_df = grouped_df.group by(grouped_df['tweet_bucket']).agg(count('user_id').alias('users_num'))

-- result_df.show()




