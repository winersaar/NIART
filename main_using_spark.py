# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
# import os

# # Step 1: Initialize a Spark session with increased memory allocation
# spark = SparkSession.builder \
#     .appName("Unique Records from JSON Files") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", "4g") \
#     .getOrCreate()

# # Step 2: Define the schema
# schema = StructType([
#     StructField("t", StringType(), True),
#     StructField("x", DoubleType(), True),
#     StructField("y", DoubleType(), True),
#     StructField("v", DoubleType(), True),
#     StructField("h", DoubleType(), True),
#     StructField("det", ArrayType(StringType()), True),  # Assuming det is an array of strings
#     StructField("n", IntegerType(), True),
#     StructField("_corrupt_record", StringType(), True)  # Include the _corrupt_record field for corrupt record handling
# ])

# # Step 3: Define the path to your folder containing JSON files
# json_folder_path = "/Users/saarwiner/Desktop/tt/ride"

# # Step 4: Define the path to save the CSV file
# csv_file_path = "/Users/saarwiner/Desktop/tt/unique_records.csv"

# # Initialize an empty DataFrame for accumulating results
# unique_df_accum = None

# # Process JSON files in batches of 10
# batch_size = 10
# num_files = 300

# json_files = "/Users/saarwiner/Desktop/tt/ride/0.json"
# # Read the JSON files into a DataFrame with the defined schema
# df = spark.read.schema(schema).json(json_files)

# df.show()

# for i in range(0, num_files, batch_size):
#     # Generate the list of JSON files for the current batch
#     json_files = [f"{json_folder_path}/{j}.json" for j in range(i, min(i + batch_size, num_files))]
#     print(f"Processing files: {json_files}")
    
#     # Read the JSON files into a DataFrame with the defined schema
#     df = spark.read.schema(schema).json(json_files)
    
#     # Show the DataFrame to debug
#     df.show()
    
#     # Remove the _corrupt_record field to focus on valid data
#     df_valid = df.drop("_corrupt_record")
    
#     # Identify unique records
#     unique_df = df_valid.dropDuplicates()
    
#     # Show the unique DataFrame to debug
#     unique_df.show()
    
#     # Append the unique records to the accumulator DataFrame
#     if unique_df_accum is None:
#         unique_df_accum = unique_df
#     else:
#         unique_df_accum = unique_df_accum.union(unique_df).dropDuplicates()

# # Convert the accumulated unique DataFrame to a Pandas DataFrame
# if unique_df_accum is not None:
#     pandas_unique_df = unique_df_accum.toPandas()
    
#     # Show the Pandas DataFrame to debug
#     print(pandas_unique_df.head())
    
#     # Save the Pandas DataFrame to a CSV file
#     if os.path.exists(csv_file_path):
#         pandas_unique_df.to_csv(csv_file_path, mode='a', header=False, index=False)
#     else:
#         pandas_unique_df.to_csv(csv_file_path, mode='w', header=True, index=False)
# else:
#     print("No unique records found.")

# # Step 5: Stop the Spark session (optional, for cleanup)
# spark.stop()
