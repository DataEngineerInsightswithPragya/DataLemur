--- Solution: SQL
SELECT
  distinct page_id
FROM pages
where page_id not in ( select distinct page_id from page_likes)
order by page_id asc
 
 
--- Solution: KQl
--pages
--| where page_id !in (page_likes | distinct page_id)
--| distinct page_id
--| order by page_id asc


--- Solution : PYSPARK

-- from pyspark.sql import SparkSession
-- from pyspark.sql.functions import col
-- from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType

-- spark = SparkSession.builder.appName('Page With No Likes').getOrCreate()

-- # Define schema for 'pages' DataFrame
-- pages_schema = StructType([
--     StructField("page_id", IntegerType(), False),
--     StructField("page_name", StringType(), False)
-- ])

-- # Sample data for 'pages'
-- pages_data = [
--     (20001, "SQL Solutions"),
--     (20045, "Brain Exercises"),
--     (20701, "Tips for Data Analysts")
-- ]

-- # Create pages DataFrame
-- pages_df = spark.createDataFrame(pages_data, schema=pages_schema)

-- # Define schema for 'page_likes' DataFrame
-- page_likes_schema = StructType([
--     StructField("user_id", IntegerType(), False),
--     StructField("page_id", IntegerType(), False),
--     StructField("liked_date", TimestampType(), False)
-- ])

-- # Sample data for 'page_likes'
-- page_likes_data = [
--     (111, 20001, "2022-04-08 00:00:00"),
--     (121, 20045, "2022-03-12 00:00:00"),
--     (156, 20001, "2022-07-25 00:00:00")
-- ]

-- # Create page_likes DataFrame
-- page_likes_df = spark.createDataFrame(page_likes_data, schema=page_likes_schema)

-- # Show the DataFrames
-- print("Pages DataFrame:")
-- pages_df.show()

-- print("Page Likes DataFrame:")
-- page_likes_df.show()


-- # Get distinct page_ids from page_likes
-- liked_page_ids = page_likes_df.select("page_id").distinct()

-- # Filter pages that are not in the liked pages
-- filter_df = pages_df.join(liked_page_ids, on="page_id", how="left_anti").distinct().orderBy("page_id")

-- # Show the result
-- filter_df.show()




















