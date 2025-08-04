# Schema Validation Issues and Fixes

## Issues Identified in Original Code

### 1. **Missing Imports**
- **Problem**: The code used PySpark functions without importing them
- **Fix**: Added all necessary imports:
  ```python
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import col, monotonically_increasing_id, lit, when, concat_ws, array_remove, array
  from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
  ```

### 2. **Undefined Variables**
- **Problem**: `spark`, `pyspark_schema`, and `avro_schema` were not defined
- **Fix**: 
  - Created `get_or_create_spark()` function to initialize Spark session
  - Created `get_sample_schema()` function for schema definition
  - Added proper schema registry integration placeholder

### 3. **Performance Issues**
- **Problem**: Using `collect()` on potentially large DataFrames can cause memory issues and performance bottlenecks
- **Fix**: Replaced all `collect()` operations with DataFrame operations:
  - Used `array_remove(array(*expressions), None)` to identify failed fields
  - Used `concat_ws()` for string concatenation
  - Eliminated row-by-row processing in favor of vectorized operations

### 4. **Inefficient Validation Logic**
- **Problem**: Multiple passes through data and inefficient filtering
- **Fix**: 
  - Combined all validation conditions into a single expression
  - Used single filter operation instead of multiple passes
  - Optimized failed record identification using array functions

### 5. **Error Handling**
- **Problem**: No error handling for file reading or processing failures
- **Fix**: Added comprehensive try-catch blocks with proper error reporting

## Key Improvements

### 1. **Optimized DataFrame Operations**
```python
# Before: Using collect() which loads all data into memory
for row in corrupt_records.collect():
    failed_record = {...}
    failed_records.append(failed_record)

# After: Pure DataFrame operations
corrupt_failed = corrupt_records_base.select(
    col("filter_row_id"),
    col("_corrupt_record").alias("validation_error"),
    lit("type_mismatch_or_parsing_error").alias("error_type"),
    lit("unknown").alias("failed_fields"),
    lit(None).cast(StringType()).alias("original_data")
)
```

### 2. **Efficient Required Field Validation**
```python
# Before: Multiple filter operations
for field in required_fields:
    validation_df = validation_df.filter(
        col(field).isNotNull() & (col(field) != "")
    )

# After: Single combined condition
all_valid_condition = validation_conditions[0]
for condition in validation_conditions[1:]:
    all_valid_condition = all_valid_condition & condition
success_df = clean_records.filter(all_valid_condition)
```

### 3. **Smart Failed Field Identification**
```python
# Using array operations to identify which fields failed
failed_fields_array = array_remove(array(*failed_field_expressions), None)
validation_error = concat_ws("", lit("Required fields are null/empty: "), 
                           concat_ws(",", col("failed_fields_array")))
```

## Files Created

1. **`schema_validation_fixed.py`**: Basic fix with proper imports and structure
2. **`optimized_schema_validation.py`**: Optimized version with performance improvements

## Usage

### Basic Usage
```python
from schema_validation_fixed import validate_with_schema_enforcement

# Update with your actual file path and schema registry URL
success_df, failed_df, pyspark_schema, avro_schema = validate_with_schema_enforcement()
```

### Optimized Usage
```python
from optimized_schema_validation import validate_with_optimized_schema_enforcement

success_df, failed_df, pyspark_schema, avro_schema = validate_with_optimized_schema_enforcement(
    file_path="s3://your-bucket/your-file.json",
    schema_registry_url="your-schema-registry-url"
)
```

## Performance Benefits

1. **Memory Efficiency**: Eliminates `collect()` operations that load data into driver memory
2. **Processing Speed**: Uses vectorized operations instead of row-by-row processing
3. **Scalability**: Can handle large datasets without memory issues
4. **Spark Optimization**: Leverages Spark's catalyst optimizer for better query planning

## Testing

The optimized version includes a test data generator and comprehensive validation testing:

```python
# Run the test
python optimized_schema_validation.py
```

This will create test data with various validation scenarios and demonstrate the validation results.

## Integration Notes

1. **Schema Registry**: Replace the `get_sample_schema()` function with your actual schema registry integration
2. **File Paths**: Update file paths to point to your actual data sources
3. **Error Handling**: Customize error handling based on your specific requirements
4. **Monitoring**: Add logging and monitoring as needed for production use