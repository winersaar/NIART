import pandas as pd
import numpy as np
import os

# Define the path to your folder containing JSON files
json_folder_path = "/Users/saarwiner/Desktop/tt/rides"
csv_outliers_path = "/Users/saarwiner/Desktop/tt/outliers.csv"

# Initialize an empty DataFrame to accumulate outliers
outliers_df = pd.DataFrame()

# Process JSON files in batches
batch_size = 10
num_files = 300

def compute_iqr(series):
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    return Q1, Q3, IQR

def filter_outliers(df, column, Q1, Q3, IQR):
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df[(df[column] < lower_bound) | (df[column] > upper_bound)]

for i in range(0, num_files, batch_size):
    batch_files = [os.path.join(json_folder_path, f"{j}.json") for j in range(i, min(i + batch_size, num_files))]
    print(f"Processing files: {batch_files}")
    
    # Read the JSON files into a pandas DataFrame
    batch_df = pd.concat([pd.read_json(file) for file in batch_files], ignore_index=True)
    
    # Show the DataFrame for debugging
    print(batch_df.head())
    
    # Compute IQR for each numerical column and identify outliers
    columns = ["x", "y", "v", "h"]
    for column in columns:
        if column in batch_df:
            series = batch_df[column].dropna()
            if len(series) > 1:
                Q1, Q3, IQR = compute_iqr(series)
                print(f"{column}: Q1 = {Q1}, Q3 = {Q3}, IQR = {IQR}")
                column_outliers = filter_outliers(batch_df, column, Q1, Q3, IQR)
                outliers_df = pd.concat([outliers_df, column_outliers]).drop_duplicates(subset=columns)
            else:
                print(f"{column}: Not enough data to compute IQR")
        else:
            print(f"{column}: Column not found in data")

# Show the outliers for debugging
print(outliers_df.head())

# Save the outliers to a CSV file
if not outliers_df.empty:
    outliers_df.to_csv(csv_outliers_path, index=False)
else:
    print("No outliers found.")
