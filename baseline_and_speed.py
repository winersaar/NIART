import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Define paths to JSON files and output folder
json_folder_path = "/Users/saarwiner/Desktop/tt/rides"
output_folder_path = "/Users/saarwiner/Desktop/tt"

# Ensure the output folder exists
os.makedirs(output_folder_path, exist_ok=True)

# Lists to hold journey IDs for different categories
normal_journeys = []
strange_journeys = {
    'no_det_slowed_or_stopped': [],
    'det_no_response': [],
    'det_with_response': []
}

# Function to parse a single JSON file into a DataFrame
def parse_journey(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

# Function to calculate baseline speed and acceleration for journeys with and without det
def calculate_baseline_metrics(json_folder_path):
    det_absent_speeds = []
    det_present_speeds = []
    det_absent_accelerations = []
    det_present_accelerations = []
    
    for i in range(300):
        file_path = os.path.join(json_folder_path, f'{i}.json')
        
        # Check if the file exists
        if not os.path.isfile(file_path):
            print(f"File not found: {file_path}")
            continue
        
        try:
            journey_df = parse_journey(file_path)
            det_present = journey_df['det'].apply(len).sum() > 0
            
            if not det_present:
                det_absent_speeds.append(journey_df['v'].mean())
                det_absent_accelerations.append(journey_df['v'].diff().mean())
            else:
                det_present_speeds.append(journey_df['v'].mean())
                det_present_accelerations.append(journey_df['v'].diff().mean())
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue
    
    avg_speed_no_det = np.mean(det_absent_speeds)
    avg_speed_det = np.mean(det_present_speeds)
    avg_acceleration_no_det = np.mean(det_absent_accelerations)
    avg_acceleration_det = np.mean(det_present_accelerations)
    
    return avg_speed_no_det, avg_speed_det, avg_acceleration_no_det, avg_acceleration_det

# Function to categorize journeys based on deviations from baselines
def categorize_journey(journey_df, avg_speed_no_det, avg_speed_det, avg_acceleration_no_det, avg_acceleration_det):
    det_present = journey_df['det'].apply(len).sum() > 0
    journey_avg_speed = journey_df['v'].mean()
    journey_avg_acceleration = journey_df['v'].diff().mean()
    
    if not det_present and abs(journey_avg_speed - avg_speed_no_det) < 5 and abs(journey_avg_acceleration - avg_acceleration_no_det) < 1:  # Adjust thresholds as needed
        return 'normal'
    if not det_present:
        return 'no_det_slowed_or_stopped'
    if det_present and abs(journey_avg_speed - avg_speed_det) < 5 and abs(journey_avg_acceleration - avg_acceleration_det) < 1:  # Adjust thresholds as needed
        return 'det_no_response'
    if det_present:
        return 'det_with_response'
    return 'normal'  # Fallback to normal if conditions are not met

# Calculate baseline metrics for journeys with and without det
avg_speed_no_det, avg_speed_det, avg_acceleration_no_det, avg_acceleration_det = calculate_baseline_metrics(json_folder_path)

# Categorize journeys using the enhanced logic
for i in range(300):
    file_path = os.path.join(json_folder_path, f'{i}.json')
    
    # Check if the file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        continue
    
    try:
        journey_df = parse_journey(file_path)
        category = categorize_journey(journey_df, avg_speed_no_det, avg_speed_det, avg_acceleration_no_det, avg_acceleration_det)
        
        if category == 'normal':
            normal_journeys.append(i)
        else:
            strange_journeys[category].append(i)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        continue

# Print categorized journey IDs
print("Normal Journeys: ", normal_journeys)
print("Strange Journeys with no det but slowed/stopped: ", strange_journeys['no_det_slowed_or_stopped'])
print("Strange Journeys with det but no response: ", strange_journeys['det_no_response'])
print("Strange Journeys with det and response: ", strange_journeys['det_with_response'])

# Plotting example for a strange journey
def plot_journey(journey_df, title):
    plt.figure(figsize=(12, 6))
    plt.plot(journey_df['t'], journey_df['v'], label='Speed (v)')
    plt.xlabel('Time')
    plt.ylabel('Speed')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()

# Example: Plotting a strange journey with det but no response
if strange_journeys['det_no_response']:
    example_journey_id = strange_journeys['det_no_response'][0]
    file_path = os.path.join(json_folder_path, f'{example_journey_id}.json')
    try:
        journey_df = parse_journey(file_path)
        plot_journey(journey_df, f'Strange Journey Example (ID: {example_journey_id}): Det but No Response')
    except Exception as e:
        print(f"Error plotting journey {example_journey_id}: {e}")
