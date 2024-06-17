import json
import pandas as pd
import os
import matplotlib.pyplot as plt

def load_and_plot_json_data(file_path):
    """
    Load JSON files from the given file path and plot the 'x', 'y', and 'v' variables for each file.

    Parameters:
    file_path (str): The path to the directory containing JSON files.
    """
    # Read each JSON file and convert it to a DataFrame
    for json_file in os.listdir(file_path):
        if json_file.endswith('.json'):
            with open(os.path.join(file_path, json_file), 'r') as file:
                data = json.load(file)
                df = pd.json_normalize(data)
                df.groupby('t')
                # Step 8: Define the path to save the CSV file
                csv_file_path = "/Users/saarwiner/Desktop/tt/stats_groupby_test.csv"

                # Step 9: Save the Pandas DataFrame to a CSV file
                df.to_csv(csv_file_path, index=False)

                # Plotting
                fig, axes = plt.subplots(3, 1, figsize=(10, 15), sharex=True)
                fig.suptitle(f'Plots for {json_file}')
                
                axes[0].plot(df['x'],  label='x', color='blue')
                axes[0].set_ylabel('x')
                axes[0].legend()

                axes[1].plot(df['y'], label='y', color='green')
                axes[1].set_ylabel('y')
                axes[1].legend()

                axes[2].plot(df['v'], label='v', color='red')
                axes[2].set_ylabel('v')
                axes[2].set_xlabel('t')
                axes[2].legend()

                plt.tight_layout(rect=[0, 0, 1, 0.96])
                plt.show()
                r = 4

# Example usage

file_path = '/Users/saarwiner/Desktop/tt/rides'
load_and_plot_json_data(file_path)