import os
import json
import csv

def find_largest_pth_file(directory):
    """
    Finds the largest .pth file in the given directory.
    Returns the size in MB or a message if no .pth file is found.
    """
    pth_files = [f for f in os.listdir(directory) if f.endswith('.pth')]
    if not pth_files:
        return "no .pth model found"  # Return a message if no .pth file is found

    largest_file = max(pth_files, key=lambda f: os.path.getsize(os.path.join(directory, f)))
    size_in_mb = os.path.getsize(os.path.join(directory, largest_file)) / (1024 * 1024)
    return size_in_mb

def extract_metrics_from_json(json_path):
    """
    Extracts the 'metric' field from the JSON file.
    """
    with open(json_path, 'r') as file:
        data = json.load(file)
    return data['metric']

def create_csv_from_model_outputs(model_outputs_dir, output_csv_path):
    """
    Creates a CSV file summarizing the model outputs.
    """
    # Prepare CSV header
    headers = ['Model Name', 'Largest Model Size (MB)']
    first_metrics = None

    # Prepare rows to write to CSV
    rows = []

    # Traverse through each subdirectory in model_outputs
    for subdir in os.listdir(model_outputs_dir):
        subdir_path = os.path.join(model_outputs_dir, subdir)

        # Check if it's a directory
        if not os.path.isdir(subdir_path):
            continue

        # Find the eval_single_scale JSON file in the directory
        json_file = next((f for f in os.listdir(subdir_path) if f.startswith('eval_single_scale_') and f.endswith('.json')), None)
        
        # Determine the largest .pth file size in MB or a message if not found
        largest_pth_size = find_largest_pth_file(subdir_path)

        if json_file:
            json_path = os.path.join(subdir_path, json_file)
            
            # Extract metrics from JSON file
            metrics = extract_metrics_from_json(json_path)
            
            # Prepare the row data
            row = [subdir, largest_pth_size] + list(metrics.values())
            
            # Save metrics keys for CSV header
            if first_metrics is None:
                first_metrics = list(metrics.keys())
                headers.extend(first_metrics)
            
        else:
            # Handle case where eval_single_scale_* file is not found
            row = [subdir, largest_pth_size, "no data found"]

        # Append the row data to rows list
        rows.append(row)

    # Write to CSV file
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV file has been created: {output_csv_path}")

# Usage
model_outputs_dir = '/mnt/e/Corrosion/model_outputs/'  # Change this to your actual model outputs directory
output_csv_path = '/mnt/e/Corrosion/model_outputs/model_outputs_summary.csv'  # Change this to your desired output CSV file path
create_csv_from_model_outputs(model_outputs_dir, output_csv_path)
