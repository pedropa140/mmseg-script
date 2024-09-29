import os
import json
import csv
import ast
from argparse import ArgumentParser

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

def parse_config_file(config_path):
    """
    Parses the config file to extract model, optimizer, training pipeline, dataset, and lr_config details.
    """
    details = {
        'pretrained': 'N/A',
        'backbone': 'N/A',
        'decode_head': 'N/A',
        'auxiliary_head': 'N/A',
        'optimizer': 'N/A',
        'learning_rate': 'N/A',
        'training_pipeline': 'N/A',
        'dataset_path': 'N/A',
        'lr_decay_policy': 'N/A',
        'min_lr': 'N/A'
    }

    with open(config_path, 'r') as file:
        content = file.read()

    # Use ast.literal_eval to safely evaluate the config content
    try:
        config_data = ast.literal_eval(content)

        # Extract model details
        if 'model' in config_data:
            model = config_data['model']
            details['pretrained'] = model.get('pretrained', 'N/A')
            details['backbone'] = model.get('backbone', {}).get('type', 'N/A')
            details['decode_head'] = model.get('decode_head', {}).get('type', 'N/A')
            details['auxiliary_head'] = model.get('auxiliary_head', {}).get('type', 'N/A')

        # Extract optimizer details
        if 'optimizer' in config_data:
            optimizer = config_data['optimizer']
            details['optimizer'] = optimizer.get('type', 'N/A')
            details['learning_rate'] = optimizer.get('lr', 'N/A')

        # Extract training pipeline details
        if 'train_pipeline' in config_data:
            train_pipeline = config_data['train_pipeline']
            details['training_pipeline'] = [item.get('type', 'N/A') for item in train_pipeline]

        # Extract dataset path details
        if 'data' in config_data:
            train_dataset = config_data['data'].get('train', {})
            details['dataset_path'] = train_dataset.get('data_root', 'N/A')

        # Extract lr_config details
        if 'lr_config' in config_data:
            lr_config = config_data['lr_config']
            details['lr_decay_policy'] = lr_config.get('policy', 'N/A')
            details['min_lr'] = lr_config.get('min_lr', 'N/A')

    except Exception as e:
        print(f"Error parsing config file {config_path}: {e}")

    return details



def create_csv_from_model_outputs(model_outputs_dir, output_csv_path):
    """
    Creates a CSV file summarizing the model outputs and config details.
    """
    # Prepare CSV header
    headers = ['Model Name', 'Largest Model Size (MB)']
    first_metrics = None
    config_headers = ['pretrained', 'backbone', 'decode_head', 'auxiliary_head', 'optimizer',
                      'learning_rate', 'training_pipeline', 'dataset_path', 'lr_decay_policy', 'min_lr']

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
        
        if json_file:
            json_path = os.path.join(subdir_path, json_file)
            
            # Extract metrics from JSON file
            metrics = extract_metrics_from_json(json_path)
            
            # Determine the largest .pth file size in MB
            largest_pth_size = find_largest_pth_file(subdir_path)
            
            # Find and parse the config file
            config_file = os.path.join(subdir_path, f"{subdir}.py")
            config_details = parse_config_file(config_file) if os.path.exists(config_file) else {}

            # Prepare the row data
            row = [subdir, largest_pth_size] + list(metrics.values())
            
            # Add configuration details to the row
            for header in config_headers:
                row.append(config_details.get(header, 'N/A'))

            # Save metrics keys for CSV header
            if first_metrics is None:
                first_metrics = list(metrics.keys())
                headers.extend(first_metrics)
                headers.extend(config_headers)
            
            # Append the row data to rows list
            rows.append(row)

    # Write to CSV file
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV file has been created: {output_csv_path}")

if __name__ == '__main__':

    argparse = ArgumentParser()
    ArgumentParser.add_argument()

    # Usage
    model_outputs_dir = '/mnt/e/Corrosion/model_outputs/'  # Change this to your actual model outputs directory
    output_csv_path = '/mnt/e/Corrosion/model_outputs/model_outputs_summary.csv'  # Change this to your desired output CSV file path
    create_csv_from_model_outputs(model_outputs_dir, output_csv_path)
