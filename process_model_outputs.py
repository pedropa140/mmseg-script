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
        'encode_head': 'N/A',
        'auxiliary_head': 'N/A',
        'optimizer': 'N/A',
        'learning_rate': 'N/A',
        'training_pipeline': 'N/A',
        'dataset_path': 'N/A',
        'lr_decay_policy': 'N/A',
        'min_lr': 'N/A'
    }

        # Use a restricted environment to execute the config file
    config_globals = {}
    config_locals = {}


    # Use ast.literal_eval to safely evaluate the config content
    try:
        # Execute the config file in the restricted environment
        with open(config_path, 'r') as file:
            exec(file.read(), config_globals, config_locals)

        # Now, extract the information from the local variables
        # Model details
        if 'model' in config_locals:
            model = config_locals['model']
            details['pretrained'] = str(model.get('pretrained', 'N/A'))


            details['backbone'] = model.get('backbone', {}).get('type', 'N/A')
            if model.get('backbone', {}).get('backbone_cfg',{}):
                details['backbone'] = model.get('backbone', {}).get('backbone_cfg',{}).get('type', {})
                details['pretrained'] = model.get('backbone', {}).get('backbone_cfg',{}).get('init_cfg', {}).get('checkpoint', {})
                

            if isinstance(model.get('decode_head'), list):
                decode_detail = []
                for head in model['decode_head']:
                    head_type = head.get('type', 'N/A')
                    head_channels = head.get('in_channels', 'N/A')
                    head_details = f"{head_type}(channels={head_channels})"
                    decode_detail.append(head_details)
                details['decode_head'] = ': '.join(decode_detail)
            else: 
                details['decode_head'] = model.get('decode_head', {}).get('type', 'N/A')
            
            #details['encode_head'] = model.get('encode_head', {}).get('type', 'N/A')
             # Handle auxiliary_head being a list
            if isinstance(model.get('auxiliary_head'), list):
                auxiliary_details = []
                for aux in model['auxiliary_head']:
                    aux_type = aux.get('type', 'N/A')
                    aux_channels = aux.get('channels', 'N/A')
                    aux_details = f"{aux_type}(channels={aux_channels})"
                    auxiliary_details.append(aux_details)
                details['auxiliary_head'] = '; '.join(auxiliary_details)
            else:
                if model.get('auxiliary_head', {}) != None:
                    details['auxiliary_head'] = model.get('auxiliary_head', {}).get('type', 'N/A')
                else:
                    details['auxiliary_head'] = model.get('auxiliary_head', {})

        # Optimizer details
        if 'optimizer' in config_locals:
            optimizer = config_locals['optimizer']
            details['optimizer'] = optimizer.get('type', 'N/A')
            details['learning_rate'] = optimizer.get('lr', 'N/A')

        # Training pipeline details
        if 'train_pipeline' in config_locals:
            train_pipeline = config_locals['train_pipeline']
            details['training_pipeline'] = [item.get('type', 'N/A') for item in train_pipeline]

        # Dataset path details
        if 'data' in config_locals:
            details['dataset_path'] = config_locals['data'].get('train', {}).get('dataset', {}).get('img_dir', {})

        # Learning rate config details
        if 'lr_config' in config_locals:
            lr_config = config_locals['lr_config']
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
    headers = ['Model Name']
    config_headers = ['pretrained', 'backbone', 'decode_head', 'auxiliary_head', 'optimizer',
                      'learning_rate', 'training_pipeline', 'dataset_path', 'lr_decay_policy', 'min_lr']
    memory_header = ['Largest Model Size (MB)']
    first_metrics = None

    # Prepare rows to write to CSV
    rows = []

    # Traverse through each subdirectory in model_outputs
    for subdir in os.listdir(model_outputs_dir):
        subdir_path = os.path.join(model_outputs_dir, subdir)

        # Check if it's a directory
        if not os.path.isdir(subdir_path):
            continue
        row = [subdir]

        # Find and parse the config file
        config_file = os.path.join(subdir_path, f"{subdir}.py")
        config_details = parse_config_file(config_file) if os.path.exists(config_file) else {}

        # Add configuration details to the row
        for header in config_headers:
            row.append(config_details.get(header, 'N/A'))


        # Determine the largest .pth file size in MB
        largest_pth_size = find_largest_pth_file(subdir_path)
        
        # Add the largest model size to the row
        row.append(largest_pth_size)


        # Find the eval_single_scale JSON file in the directory
        json_file = next((f for f in os.listdir(subdir_path) if f.startswith('eval_single_scale_') and f.endswith('.json')), None)

        if json_file:
            json_path = os.path.join(subdir_path, json_file)
            
            # Extract metrics from JSON file
            metrics = extract_metrics_from_json(json_path)

            # Append the metrics values to the row
            row.extend(list(metrics.values()))
            
            # Save metrics keys for CSV header
            if first_metrics is None:
                first_metrics = list(metrics.keys())
                headers.extend(config_headers)
                headers.extend(memory_header)
                headers.extend(first_metrics)
            
        # Append the row data to rows list
        rows.append(row)

    # Write to CSV file
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV file has been created: {output_csv_path}")

if __name__ == '__main__':

    parser = ArgumentParser(description="Look through the trained model outputs to extract relevant information")
    parser.add_argument('model_out_dir', type=str, help='Directory that contains folders of all the models', default=None)
    parser.add_argument('--output_file_location', type=str, 
                        help='Location to output the csv file. It will place the file in the current directory by default', 
                        default='./model_outputs_summary.csv')
    args_out = parser.parse_args()
    create_csv_from_model_outputs(args_out.model_out_dir, args_out.output_file_location)
    # Usage
    # model_outputs_dir = '/mnt/e/Corrosion/model_outputs/'  # Change this to your actual model outputs directory
    # output_csv_path = '/mnt/e/Corrosion/model_outputs/model_outputs_summary.csv'  # Change this to your desired output CSV file path
    # create_csv_from_model_outputs(model_outputs_dir, output_csv_path)
