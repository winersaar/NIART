# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
# from pyspark.sql.functions import col
# import pandas as pd

# # Initialize a Spark session
# spark = SparkSession.builder \
#     .appName("Outliers Detection from JSON Files") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", "4g") \
#     .getOrCreate()

# # Define the schema
# schema = StructType([
#     StructField("t", StringType(), True),
#     StructField("x", DoubleType(), True),
#     StructField("y", DoubleType(), True),
#     StructField("v", DoubleType(), True),
#     StructField("h", DoubleType(), True),
#     StructField("det", ArrayType(StringType()), True),  # Assuming det is an array of strings
#     StructField("n", IntegerType(), True)
# ])

# # Define the path to your folder containing JSON files
# json_folder_path = "/Users/saarwiner/Desktop/tt/rides"

# # Read all JSON files into a DataFrame with the defined schema
# df = spark.read.schema(schema).json(json_folder_path)

# # Compute the IQR for each numerical column
# def compute_iqr(df, column):
#     quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
#     if len(quantiles) < 2:
#         return None, None, None
#     Q1, Q3 = quantiles
#     IQR = Q3 - Q1
#     return Q1, Q3, IQR

# columns = ["x", "y", "v", "h"]
# iqr_values = {col: compute_iqr(df, col) for col in columns}

# # Print IQR values for debugging
# for col, (Q1, Q3, IQR) in iqr_values.items():
#     if Q1 is not None and Q3 is not None:
#         print(f"{col}: Q1 = {Q1}, Q3 = {Q3}, IQR = {IQR}")
#     else:
#         print(f"{col}: Not enough data to compute IQR")

# # Filter out the outliers
# def filter_outliers(df, column, Q1, Q3, IQR):
#     if Q1 is None or Q3 is None:
#         return df
#     lower_bound = Q1 - 1.5 * IQR
#     upper_bound = Q3 + 1.5 * IQR
#     return df.filter((col(column) < lower_bound) | (col(column) > upper_bound))

# outliers = None
# for column in columns:
#     Q1, Q3, IQR = iqr_values[column]
#     if Q1 is not None and Q3 is not None:
#         column_outliers = filter_outliers(df, column, Q1, Q3, IQR)
#         if outliers is None:
#             outliers = column_outliers
#         else:
#             outliers = outliers.union(column_outliers).dropDuplicates()

# # Show the outliers
# if outliers is not None:
#     outliers.show()
#     # Convert the outliers DataFrame to a Pandas DataFrame
#     pandas_outliers_df = outliers.toPandas()

#     # Define the path to save the outliers to a CSV file
#     csv_outliers_path = "/Users/saarwiner/Desktop/tt/outliers.csv"

#     # Save the Pandas DataFrame to a CSV file
#     pandas_outliers_df.to_csv(csv_outliers_path, index=False)
# else:
#     print("No outliers found.")

# # Stop the Spark session
# spark.stop()
