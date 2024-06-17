import json
import pandas as pd
import os

def load_json_files_and_generate_stats(file_path):
    """
    Load JSON files from the given file path, combine them into a DataFrame,
    and generate a statistical summary table.

    Parameters:
    file_path (str): The path to the directory containing JSON files.

    Returns:
    pd.DataFrame: A statistical summary table of the combined JSON data.
    """
    # List to hold DataFrames
    data_frames = []

    # Read each JSON file and convert it to a DataFrame
    for json_file in os.listdir(file_path):
        if json_file.endswith('.json'):
            with open(os.path.join(file_path, json_file), 'r') as file:
                data = json.load(file)
                df = pd.json_normalize(data)
                data_frames.append(df)

    # Combine all DataFrames into one DataFrame
    combined_df = pd.concat(data_frames, keys=[file for file in os.listdir(file_path) if file.endswith('.json')])

    # # Generate statistical summary
    stats = combined_df
    # stats = combined_df.groupby(level=0).describe()

    # Step 8: Define the path to save the CSV file
    csv_file_path = "/Users/saarwiner/Desktop/tt/stats_0-35.csv"

    # Step 9: Save the Pandas DataFrame to a CSV file
    stats.to_csv(csv_file_path, index=False)
    return stats

# Example usage
file_path = '/Users/saarwiner/Desktop/tt/ride'
stats_table = load_json_files_and_generate_stats(file_path)
# stats_table.show()
print(stats_table)

# # Step 2: Define the path to your folder containing JSON files
# json_folder_path = "/Users/saarwiner/Desktop/tt/rides"

