--- Solution : SQL
SELECT
  part,
  assembly_step
FROM parts_assembly
where finish_date ISNULL

--- Solution : KQL
-- parts_assembly
-- | where isnull(finish_date)
-- | project part, assembly_step


--- Solution : PYSPARK

-- from pyspark.sql import SparkSession
-- from pyspark.sql.types import StructType,StructField,IntegerType,StringType

-- spark = SparkSession.builder.appName('Unfinished Parts').getOrCreate()

-- # Define schema for parts_assembly
-- parts_assembly_schema = StructType([
--     StructField("part", StringType(), False),
--     StructField("finish_date", TimestampType(), True),  # Nullable because some dates are missing
--     StructField("assembly_step", IntegerType(), False)
-- ])

-- # Sample data for parts_assembly
-- parts_assembly_data = [
--     ("battery", "2022-01-22 00:00:00", 1),
--     ("battery", "2022-02-22 00:00:00", 2),
--     ("battery", "2022-03-22 00:00:00", 3),
--     ("bumper", "2022-01-22 00:00:00", 1),
--     ("bumper", "2022-02-22 00:00:00", 2),
--     ("bumper", None, 3),
--     ("bumper", None, 4)
-- ]

-- # Create DataFrame
-- parts_assembly_df = spark.createDataFrame(parts_assembly_data, schema=parts_assembly_schema)

-- # Show the DataFrame
-- parts_assembly_df.show()

-- result_df = parts_assembly_df.select('part','assembly_step').where(parts_assembly_df['finish_date'].isNull())

-- result_df.show()








