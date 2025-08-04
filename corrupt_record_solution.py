"""
PySpark _corrupt_record Column Not Appearing - Solutions and Explanations

The main reasons why your _corrupt_record column is not being created:

1. INCLUDING _corrupt_record IN YOUR SCHEMA DEFINITION
   - When you define _corrupt_record as part of your schema, Spark treats it as a regular column
   - Spark won't populate it with corrupt data because it's expecting it as normal data

2. SCHEMA TOO PERMISSIVE
   - If all fields are StringType, there's no "corruption" from Spark's perspective
   - You need strict data types (IntegerType, DateType, etc.) to catch type mismatches

3. NO ACTUAL CORRUPTION DETECTED
   - Records might be valid according to your schema even if they seem "wrong" to you
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark
spark = SparkSession.builder.appName("CorruptRecordDemo").getOrCreate()

# Sample JSON data with various issues
sample_json_lines = [
    '{"name": "John", "age": 30, "city": "New York"}',          # Valid
    '{"name": "Jane", "age": 25, "city": "Boston"}',           # Valid  
    '{"name": "Bob", "age": "invalid_age", "city": "Chicago"}', # Invalid age type
    '{"name": "Alice", "age": 35}',                            # Missing city
    '{"name": "Charlie", "age": 40, "city": "Seattle", "extra": "field"}', # Extra field
    '{"invalid_json": missing_quotes}',                         # Invalid JSON
    '{"name": "David", "city": "Miami"}',                      # Missing age
]

# Write test data
with open("test_data.json", "w") as f:
    for line in sample_json_lines:
        f.write(line + "\n")

print("=== PROBLEM: Schema includes _corrupt_record ===")
# THIS IS WRONG - including _corrupt_record in schema
problematic_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("_corrupt_record", StringType(), True)  # DON'T DO THIS!
])

df_wrong = (spark.read
            .schema(problematic_schema)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json("test_data.json"))

print("Columns:", df_wrong.columns)
print("Corrupt records count:", df_wrong.filter(df_wrong._corrupt_record.isNotNull()).count())
print("Why: Spark treats _corrupt_record as a regular column, not for corruption tracking\n")

print("=== SOLUTION 1: Remove _corrupt_record from schema ===")
# CORRECT APPROACH - don't include _corrupt_record in schema
correct_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),    # Strict type to catch "invalid_age"
    StructField("city", StringType(), True)
])

df_correct = (spark.read
              .schema(correct_schema)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .json("test_data.json"))

print("Columns:", df_correct.columns)
if "_corrupt_record" in df_correct.columns:
    corrupt_count = df_correct.filter(df_correct._corrupt_record.isNotNull()).count()
    print(f"Corrupt records count: {corrupt_count}")
    
    if corrupt_count > 0:
        print("Corrupt records:")
        df_correct.filter(df_correct._corrupt_record.isNotNull()).show(truncate=False)
else:
    print("_corrupt_record column still not found - might be no corruption detected")

print("\n=== SOLUTION 2: Let Spark infer schema ===")
# Alternative: Let Spark infer schema, then apply strict validation
df_inferred = (spark.read
               .option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", "_corrupt_record")
               .json("test_data.json"))

print("Inferred schema:")
df_inferred.printSchema()
print("Columns:", df_inferred.columns)

if "_corrupt_record" in df_inferred.columns:
    corrupt_count = df_inferred.filter(df_inferred._corrupt_record.isNotNull()).count()
    print(f"Corrupt records count: {corrupt_count}")
    if corrupt_count > 0:
        print("Corrupt records:")
        df_inferred.filter(df_inferred._corrupt_record.isNotNull()).show(truncate=False)

print("\n=== SOLUTION 3: Alternative validation approach ===")
# If _corrupt_record still doesn't work, implement custom validation
from pyspark.sql.functions import col, when, isnan, isnull

df_clean = spark.read.json("test_data.json")
print("Original data:")
df_clean.show()

# Custom validation - identify problematic records
df_validated = df_clean.withColumn(
    "is_valid",
    when(
        (col("name").isNull()) | 
        (col("age").isNull()) | 
        (col("city").isNull()) |
        (~col("age").cast("integer").isNotNull()), 
        False
    ).otherwise(True)
)

print("With custom validation:")
df_validated.show()

print("Invalid records:")
df_validated.filter(~col("is_valid")).show()

print("""
KEY TAKEAWAYS:
1. NEVER include '_corrupt_record' in your schema definition
2. Use strict data types (IntegerType vs StringType) to catch type mismatches  
3. If corruption isn't detected, implement custom validation logic
4. Consider using mode='DROPMALFORMED' or mode='FAILFAST' for different behaviors
5. The _corrupt_record column only appears when Spark detects actual schema violations

YOUR CORRECTED CODE SHOULD BE:

# Define schema WITHOUT _corrupt_record
schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", IntegerType(), True),  # Use strict types
    # ... other fields
    # DON'T include StructField("_corrupt_record", StringType(), True)
])

df = (spark.read
      .schema(schema)  # Schema without _corrupt_record
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .json(file_path))
""")

spark.stop()