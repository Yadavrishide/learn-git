from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, lit, when, concat_ws, 
    array_remove, array, coalesce, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
import json

def get_or_create_spark():
    """Initialize or get existing Spark session"""
    return SparkSession.builder \
        .appName("OptimizedSchemaValidation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def get_sample_schema():
    """Sample schema - replace with your actual schema"""
    return StructType([
        StructField("id", IntegerType(), False),      # Required
        StructField("name", StringType(), False),     # Required
        StructField("email", StringType(), True),     # Optional
        StructField("age", IntegerType(), True),      # Optional
        StructField("salary", DoubleType(), True)     # Optional
    ])

def validate_with_optimized_schema_enforcement(file_path=None, schema_registry_url=None):
    """
    Optimized schema validation function that avoids collect() operations
    and processes everything using DataFrame operations for better performance
    """
    # Initialize Spark
    spark = get_or_create_spark()
    
    # Get schema (replace with your schema registry logic)
    pyspark_schema = get_sample_schema()
    avro_schema = None  # Add your avro schema logic here
    
    # Use provided file path or default
    if file_path is None:
        file_path = "s3://your-bucket/sample-file-corrupt.json"
    
    try:
        # Read data with schema enforcement
        df_with_schema = (spark.read.schema(pyspark_schema)
                         .option("mode", "PERMISSIVE")
                         .option("columnNameOfCorruptRecord", "_corrupt_record")
                         .option("multiline", "true")
                         .json(file_path))
        
        # Add unique ID for tracking
        df_with_id = df_with_schema.withColumn("filter_row_id", monotonically_increasing_id())
        
        # Get required fields
        required_fields = [f.name for f in pyspark_schema.fields if not f.nullable]
        
        # Check if corrupt record column exists
        has_corrupt_column = "_corrupt_record" in df_with_id.columns
        
        # Separate clean and corrupt records
        if has_corrupt_column:
            clean_records = df_with_id.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            corrupt_records_base = df_with_id.filter(col("_corrupt_record").isNotNull())
        else:
            clean_records = df_with_id
            corrupt_records_base = spark.createDataFrame([], df_with_id.schema.add("_corrupt_record", StringType()))
        
        # Create validation conditions for required fields
        validation_conditions = []
        failed_field_expressions = []
        
        for field in required_fields:
            if field in clean_records.columns:
                # Condition for valid records (not null and not empty string)
                valid_condition = col(field).isNotNull() & (col(field) != "")
                validation_conditions.append(valid_condition)
                
                # Expression to identify failed fields
                failed_field_expressions.append(
                    when(~valid_condition, lit(field)).otherwise(lit(None))
                )
        
        # Apply all validation conditions at once
        if validation_conditions:
            # Records that pass all validations
            all_valid_condition = validation_conditions[0]
            for condition in validation_conditions[1:]:
                all_valid_condition = all_valid_condition & condition
            
            success_df = clean_records.filter(all_valid_condition)
            
            # Records that fail validation
            validation_failed_records = clean_records.filter(~all_valid_condition)
            
            # Create failed fields array and filter out nulls
            if failed_field_expressions:
                failed_fields_array = array_remove(array(*failed_field_expressions), None)
                
                validation_failed_with_errors = validation_failed_records.withColumn(
                    "failed_fields_array", failed_fields_array
                ).withColumn(
                    "validation_error", 
                    concat_ws("", lit("Required fields are null/empty: "), 
                             concat_ws(",", col("failed_fields_array")))
                ).withColumn(
                    "error_type", lit("required_field_validation_failed")
                ).withColumn(
                    "failed_fields", concat_ws(",", col("failed_fields_array"))
                ).withColumn(
                    "original_data", lit(None).cast(StringType())
                ).select(
                    "filter_row_id", "validation_error", "error_type", "failed_fields", "original_data"
                )
            else:
                # No required fields to validate
                validation_failed_with_errors = spark.createDataFrame([], StructType([
                    StructField('filter_row_id', LongType(), True),
                    StructField('validation_error', StringType(), True),
                    StructField('error_type', StringType(), True),
                    StructField('failed_fields', StringType(), True),
                    StructField('original_data', StringType(), True)
                ]))
        else:
            # No required fields, all clean records are successful
            success_df = clean_records
            validation_failed_with_errors = spark.createDataFrame([], StructType([
                StructField('filter_row_id', LongType(), True),
                StructField('validation_error', StringType(), True),
                StructField('error_type', StringType(), True),
                StructField('failed_fields', StringType(), True),
                StructField('original_data', StringType(), True)
            ]))
        
        # Process corrupt records
        if has_corrupt_column:
            corrupt_failed = corrupt_records_base.select(
                col("filter_row_id"),
                col("_corrupt_record").alias("validation_error"),
                lit("type_mismatch_or_parsing_error").alias("error_type"),
                lit("unknown").alias("failed_fields"),
                lit(None).cast(StringType()).alias("original_data")
            )
        else:
            corrupt_failed = spark.createDataFrame([], StructType([
                StructField('filter_row_id', LongType(), True),
                StructField('validation_error', StringType(), True),
                StructField('error_type', StringType(), True),
                StructField('failed_fields', StringType(), True),
                StructField('original_data', StringType(), True)
            ]))
        
        # Union all failed records
        failed_df = corrupt_failed.union(validation_failed_with_errors)
        
        # Remove the temporary ID column from success DataFrame
        final_success_df = success_df.drop("filter_row_id")
        
        return final_success_df, failed_df, pyspark_schema, avro_schema
        
    except Exception as e:
        print(f"Error in schema validation: {str(e)}")
        # Return empty DataFrames in case of error
        empty_success_df = spark.createDataFrame([], pyspark_schema)
        empty_failed_df = spark.createDataFrame([], StructType([
            StructField('filter_row_id', LongType(), True),
            StructField('validation_error', StringType(), True),
            StructField('error_type', StringType(), True),
            StructField('failed_fields', StringType(), True),
            StructField('original_data', StringType(), True)
        ]))
        
        return empty_success_df, empty_failed_df, pyspark_schema, avro_schema

def create_test_data(spark, file_path):
    """Helper function to create test data for validation"""
    test_data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "salary": 50000.0},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25, "salary": 60000.0},
        {"id": None, "name": "Invalid User", "email": "invalid@example.com", "age": 35, "salary": 45000.0},  # Invalid: missing required id
        {"id": 4, "name": "", "email": "empty@example.com", "age": 40, "salary": 55000.0},  # Invalid: empty required name
        {"id": 5, "name": "Valid User", "email": None, "age": None, "salary": None},  # Valid: optional fields can be null
    ]
    
    # Write test data to file (for local testing)
    if file_path.startswith("file://") or not file_path.startswith("s3://"):
        import json
        with open(file_path.replace("file://", ""), 'w') as f:
            for record in test_data:
                f.write(json.dumps(record) + '\n')

# Usage example with comprehensive testing
if __name__ == "__main__":
    spark = get_or_create_spark()
    
    # For testing purposes, create local test file
    test_file_path = "file:///tmp/test_data.json"
    local_file_path = "/tmp/test_data.json"
    
    try:
        # Create test data
        create_test_data(spark, local_file_path)
        
        print("Running optimized schema validation...")
        success_df, failed_df, pyspark_schema, avro_schema = validate_with_optimized_schema_enforcement(
            file_path=test_file_path
        )
        
        print(f"\nValidation Results:")
        print(f"Success records count: {success_df.count()}")
        print(f"Failed records count: {failed_df.count()}")
        
        print(f"\nSchema used:")
        pyspark_schema.printTreeString()
        
        print(f"\nSuccess records:")
        success_df.show(truncate=False)
        
        print(f"\nFailed records:")
        failed_df.show(truncate=False)
        
        # Show validation summary
        print(f"\nValidation Summary:")
        failed_df.groupBy("error_type").count().show()
        
    except Exception as e:
        print(f"Error running validation: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up test file
        import os
        if os.path.exists(local_file_path):
            os.remove(local_file_path)