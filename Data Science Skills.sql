---SOLUTION : SQL
SELECT 
  candidate_id 
FROM candidates 
WHERE skill in ('Python','Tableau','PostgreSQL')
GROUP BY candidate_id
HAVING count(distinct skill) = 3
order by candidate_id 



---SOLUTION : KQL
--candidates
--| where skill in ("Python", "Tableau", "PostgreSQL")
--| summarize distinct_skills = dcount(skill) by candidate_id
--| where distinct_skills == 3
--| order by candidate_id



---SOLUTION : PYSPARK
-- from pyspark.sql import SparkSession
-- from pyspark.sql.types import StructType, StructField, IntegerType, StringType
-- from pyspark.sql.functions import col, countDistinct

-- # Initialize Spark session
-- spark = SparkSession.builder.appName("CandidateSkills").getOrCreate()

-- # Define schema using StructType and StructField
-- schema = StructType([
--     StructField("candidate_id", IntegerType(), False),
--     StructField("skill", StringType(), False)
-- ])

-- # Sample data
-- data = [
--     (123, "Python"),
--     (123, "Tableau"),
--     (123, "PostgreSQL"),
--     (234, "R"),
--     (234, "PowerBI"),
--     (234, "SQL Server"),
--     (345, "Python"),
--     (345, "Tableau")
-- ]

-- # Create DataFrame with the defined schema
-- df = spark.createDataFrame(data, schema)

-- # Filter, group, and find candidates with all three skills
-- result_df = (df.filter(col("skill").isin("Python", "Tableau", "PostgreSQL"))
--              .groupBy("candidate_id")
--              .agg(countDistinct("skill").alias("distinct_skill_count"))
--              .filter(col("distinct_skill_count") == 3)
--              .orderBy("candidate_id"))

-- # Show the result
-- result_df.show()




