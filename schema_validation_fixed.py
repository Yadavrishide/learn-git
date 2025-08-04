from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lit, when, concat_ws, collect_list
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
import json

# Initialize Spark session (if not already initialized)
def get_or_create_spark():
    return SparkSession.builder \
        .appName("SchemaValidation") \
        .getOrCreate()

def get_sample_schema():
    """
    Sample schema definition - replace with your actual schema from registry
    """
    return StructType([
        StructField("id", IntegerType(), False),  # Required field
        StructField("name", StringType(), False),  # Required field
        StructField("email", StringType(), True),  # Optional field
        StructField("age", IntegerType(), True),   # Optional field
        StructField("salary", DoubleType(), True)  # Optional field
    ])

def get_schema_from_registry(schema_registry_url):
    """
    Placeholder for schema registry integration
    Replace this with your actual schema registry logic
    """
    pyspark_schema = get_sample_schema()
    avro_schema = None  # Add your avro schema logic here
    return pyspark_schema, avro_schema

def validate_with_schema_enforcement():
    # Initialize Spark
    spark = get_or_create_spark()
    
    # Get schema from registry
    # Uncomment and modify the line below when you have schema registry setup
    # pyspark_schema, avro_schema = get_schema_from_registry(schema_registry_url)
    
    # For now, using sample schema
    pyspark_schema, avro_schema = get_sample_schema(), None
    
    # Update file path to a valid one for testing
    file_path = "s3://your-bucket/sample-file-corrupt.json"  # Update this path
    
    try:
        # Method 1: Read with schema enforcement
        df_with_schema = (spark.read.schema(pyspark_schema)
                         .option("mode", "PERMISSIVE")
                         .option("columnNameOfCorruptRecord", "_corrupt_record")
                         .option("multiline", "true")
                         .json(file_path))
        
        df_with_id = df_with_schema.withColumn("filter_row_id", monotonically_increasing_id())
        
        # Check if corrupt record column exists
        has_corrupt_column = "_corrupt_record" in df_with_id.columns
        
        if has_corrupt_column:
            # Separate corrupt records (type mismatches, parsing errors)
            corrupt_records = df_with_id.filter(col("_corrupt_record").isNotNull())
            clean_records = df_with_id.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        else:
            # Create empty corrupt records DataFrame
            corrupt_schema = StructType([
                StructField("filter_row_id", LongType(), True),
                StructField("_corrupt_record", StringType(), True)
            ])
            corrupt_records = spark.createDataFrame([], corrupt_schema)
            clean_records = df_with_id
        
        # Get required fields from schema
        required_fields = [f.name for f in pyspark_schema.fields if not f.nullable]
        
        # Additional validation for required fields using DataFrame operations
        validation_df = clean_records
        failed_conditions = []
        
        for field in required_fields:
            if field in clean_records.columns:
                # Create condition for failed validation
                field_condition = col(field).isNull() | (col(field) == "")
                failed_conditions.append(
                    when(field_condition, lit(field)).otherwise(lit(None))
                )
                
                # Filter out records with null/empty required fields
                validation_df = validation_df.filter(
                    col(field).isNotNull() & 
                    (col(field) != "")
                )
        
        # Final success DataFrame
        success_df = validation_df
        
        # Create failed DataFrame more efficiently
        failed_df_list = []
        
        # Process corrupt records efficiently
        if has_corrupt_column and corrupt_records.count() > 0:
            corrupt_failed = corrupt_records.select(
                col("filter_row_id"),
                col("_corrupt_record").alias("validation_error"),
                lit("type_mismatch_or_parsing_error").alias("error_type"),
                lit("unknown").alias("failed_fields"),
                lit(None).cast(StringType()).alias("original_data")
            )
            failed_df_list.append(corrupt_failed)
        
        # Find records that failed required field validation more efficiently
        if required_fields:
            all_clean_ids = clean_records.select("filter_row_id")
            success_ids = success_df.select("filter_row_id")
            failed_validation_ids = all_clean_ids.subtract(success_ids)
            
            if failed_validation_ids.count() > 0:
                failed_validation_records = clean_records.join(failed_validation_ids, "filter_row_id", "inner")
                
                # Create failed fields list more efficiently
                failed_fields_expr = concat_ws(",", *[
                    when(col(field).isNull() | (col(field) == ""), lit(field))
                    for field in required_fields if field in clean_records.columns
                ])
                
                validation_failed = failed_validation_records.select(
                    col("filter_row_id"),
                    concat_ws("", lit("Required fields are null/empty: ["), 
                             failed_fields_expr, lit("]")).alias("validation_error"),
                    lit("required_field_validation_failed").alias("error_type"),
                    failed_fields_expr.alias("failed_fields"),
                    lit(None).cast(StringType()).alias("original_data")  # Avoid collecting large data
                )
                failed_df_list.append(validation_failed)
        
        # Union all failed DataFrames
        if failed_df_list:
            failed_df = failed_df_list[0]
            for df in failed_df_list[1:]:
                failed_df = failed_df.union(df)
        else:
            # Create empty failed DataFrame with proper schema
            failed_schema = StructType([
                StructField('filter_row_id', LongType(), True),
                StructField('validation_error', StringType(), True),
                StructField('error_type', StringType(), True),
                StructField('failed_fields', StringType(), True),
                StructField('original_data', StringType(), True)
            ])
            failed_df = spark.createDataFrame([], failed_schema)
        
        return success_df.drop("filter_row_id"), failed_df, pyspark_schema, avro_schema
        
    except Exception as e:
        print(f"Error in schema validation: {str(e)}")
        # Return empty DataFrames in case of error
        empty_success_schema = pyspark_schema
        empty_failed_schema = StructType([
            StructField('filter_row_id', LongType(), True),
            StructField('validation_error', StringType(), True),
            StructField('error_type', StringType(), True),
            StructField('failed_fields', StringType(), True),
            StructField('original_data', StringType(), True)
        ])
        
        spark = get_or_create_spark()
        empty_success_df = spark.createDataFrame([], empty_success_schema)
        empty_failed_df = spark.createDataFrame([], empty_failed_schema)
        
        return empty_success_df, empty_failed_df, pyspark_schema, avro_schema

# Usage example
if __name__ == "__main__":
    try:
        success_df, failed_df, pyspark_schema, avro_schema = validate_with_schema_enforcement()
        
        print("Success records count:", success_df.count())
        print("Failed records count:", failed_df.count())
        
        # Show sample results
        print("\nSuccess records sample:")
        success_df.show(5, truncate=False)
        
        print("\nFailed records sample:")
        failed_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"Error running validation: {str(e)}")