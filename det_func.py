import json
import pandas as pd
import os

def check_det_entries(file_path):
    """
    Check if any JSON files in the given path have entries in the 'det' variable.

    Parameters:
    file_path (str): The path to the directory containing JSON files.

    Returns:
    dict: A dictionary with filenames as keys and the number of 'det' entries as values.
    """
    det_entries = {}
    json_files = [file for file in os.listdir(file_path) if file.endswith('.json')]

    # Read each JSON file and check for 'det' entries
    for json_file in json_files:
        with open(os.path.join(file_path, json_file), 'r') as file:
            data = json.load(file)
            df = pd.json_normalize(data)

            if 'det' in df.columns:
                det_entries[json_file] = df['det'].apply(lambda x: len(x) > 0).sum()
            else:
                det_entries[json_file] = 0

    return det_entries

# Example usage
file_path = '/Users/saarwiner/Desktop/tt/rides'
det_entries = check_det_entries(file_path)

df = pd.DataFrame.from_dict(det_entries, orient='index', columns=['dt_count'])

# selecting rows where dt is > then 0 
rslt_df = df.loc[df['dt_count'] > 0] 
   
print('\n Result dataframe :\n', rslt_df)

# s = pd.Series(det_entries, name='dt_count')
# df.index.name = 'ride_number'
# df.reset_index()

# saved it on my pc, thats why we dont need it anymore
# new = pd.DataFrame.from_dict(data=det_entries,orient='index')
# # Step 8: Define the path to save the CSV file
# csv_file_path = "/Users/saarwiner/Desktop/tt/stats_dt_count.csv"

# Step 9: Save the Pandas DataFrame to a CSV file
# new.to_csv(csv_file_path, index=True)

# print(det_entries)