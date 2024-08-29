import config as cfg
import logging
import os
import json
from collections import defaultdict
import remote_operations as rops

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_red(text):
    print(f"\033[91m{text}\033[0m")

def print_blue(text):
    print(f"\033[38;2;50;128;128m{text}\033[0m")

def create_json(ssh):
    base_dir = '/'.join(os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION).replace("\\", "/").split('/')[:-1])
    print(f"Base Directory: {base_dir}")
    status_directories = {
        '_QUEUED': 'QUEUED',
        '_RUNNING': 'RUNNING',
        '_ERROR': 'ERROR',
        '_COMPLETED': 'COMPLETED',
        '_FINISHED': 'FINISHED'
    }
    
    # Open json file to check which files are already accounted for. 
    dictionary_list = []
    cfg.json_file_path = 'batch_files.json'
    logging.info(f"create_json(): Comparing batch files found in {base_dir} and {cfg.json_file_path}")
    if os.path.exists(cfg.json_file_path):
        logging.info(f"Found a premade JSON file for batch_files at {cfg.json_file_path}")
        with open(cfg.json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    # populate list of files that are found in the json file
    existing_filenames = {entry['filename'] for entry in dictionary_list}
    
    # Iterate over each status directory
    for sub_dir, status in status_directories.items():
        # Build the full path to the directory
        remote_dir = f"{base_dir}/{sub_dir}" 
    # Find the batch files stored in the remote batch file location
        print(f"Checking Directory: {remote_dir}")
        logging.info(f"Checking Directory: {remote_dir}")
        # Execute command to list files in the directory
        logging.info(f"Executing: cd {remote_dir}; ls -l")
        stdin, stdout, stderr = ssh.exec_command(f'cd {remote_dir}; ls -l')
        
        for counter, line in enumerate(stdout):
            if counter == 0:
                continue
            
            # Split the line by whitespace
            parts = line.strip().split()
            
            # Check if the entry is a file (not a directory)
            if len(parts) >= 9 and parts[0].startswith('-'):  # Files have '-' at the start of the permission string
                filename = parts[8]
                    
            # Determine which files already were found in the json file
                if filename in existing_filenames:
                    print_red(f"File {filename} is already in the JSON file.")
                    logging.info(f"File {filename} is already in the JSON file.")
                    continue
                
                # Extract the job names from the batch files to add into the json file if the fileis not already found in the JSON file
                logging.info(f"Executing: cat {remote_dir}/{filename}")
                print(f'Running Command: cat {remote_dir}/{filename}')
                stdin, stdout, stderr = ssh.exec_command(f'cat {remote_dir}/{filename}')
                job_name = ""
                working_directory = ""
                if stderr:
                    print_red("Error in creating JSON:" + str(stderr.read().decode().strip()))
                    logging.error(f"Error in creating JSON:" + str(stderr.read().decode().strip()))
                for line in stdout:
                    line = line.strip()
                    if "#SBATCH --job-name=" in line:
                        job_name = line.replace('#SBATCH --job-name=', '').replace(' ', '')

                    if "python3 ~/mmseg-personal/tools/train.py" in line:
                        working_line = line.replace('python3 ~/mmseg-personal/tools/train.py ~', '').replace('.py', '').split("/")
                        working_length = len(working_line)
                        working_directory = working_line[working_length - 1]
                
                file_dict = {
                    'filename': filename,
                    'job_name': job_name,
                    'working_directory': working_directory,
                    'status': status
                }
                
                # Add new files to the json file to keep track of which files are run
                dictionary_list.append(file_dict)
                existing_filenames.add(filename)
                print_green(f"Added file {filename} to the JSON file.")
                logging.info(f"Added file {filename} to the JSON file.")
    
    with open(cfg.json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)

def update_json(ssh):
    logging.info("Updating Json file")
    # TODO REMOVE FILES FORM JSON FILE IF BATCH FILES ARE MOVED/NOT PRESENT
    dictionary_list = []
    folder_list = []
    directory_dictionary = {}
    project_work_dir = f'{cfg.REMOTE_WORKING_PROJECT}/{cfg.REMOTE_WORK_DIR}'
    folder_directory = os.path.join(cfg.REMOTE_WORKING_PROJECT, *cfg.REMOTE_BATCH_FILE_LOCATION.split('/')[:-1]).replace("\\", "/")

    if os.path.exists(cfg.json_file_path):
        with open(cfg.json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)

    logging.info(f"Executing: 'cd {folder_directory} ; ls -l | grep \'^d\' | grep \' \_\''")
    stdin, stdout, stderr = ssh.exec_command(f'cd {folder_directory} ; ls -l | grep \'^d\' | grep \' \_\'')
    for counter, line in enumerate(stdout):
        folder_list.append(line.split()[-1])
    
    for folder in folder_list:
        logging.info(f"Executing: 'cd {folder_directory}/{folder} ; ls -l'")
        stdin, stdout, stderr = ssh.exec_command(f'cd {folder_directory}/{folder} ; ls -l')
        folder_files = []
        for counter, line in enumerate(stdout):
            if counter == 0:
                continue
            folder_files.append(line.split()[-1])
        directory_dictionary[folder.replace("_", "")] = folder_files

    status_dictionary = {}
    for job in dictionary_list:
        status = job['status']
        filename = job['filename']
        
        # Add the filename to the appropriate status list in the dictionary
        if status not in status_dictionary:
            status_dictionary[status] = []
        status_dictionary[status].append(filename)

    def compare_status_dicts(dict1, dict2):
        """
        Compare two dictionaries where keys represent status and values are lists of items.
        The function checks if the dictionaries are equal, regardless of the order of the items in the lists.
        """
        def normalize_dict(d):
            normalized = defaultdict(set)
            for key, values in d.items():
                normalized[key] = set(values)
            return normalized

        normalized_dict1 = normalize_dict(dict1)
        normalized_dict2 = normalize_dict(dict2)

        return normalized_dict1 == normalized_dict2
    
    def remove_empty_lists(d):
        """
        Remove keys from the dictionary where the value is an empty list.
        """
        return {k: v for k, v in d.items() if v}

    directory_dictionary = remove_empty_lists(directory_dictionary)

    status_similiarity = compare_status_dicts(status_dictionary, directory_dictionary)

    if not status_similiarity:
        # Create a reverse lookup from directory_dictionary
        reverse_lookup = {}
        for status, files in directory_dictionary.items():
            for file in files:
                reverse_lookup[file] = status
        
        # Update the statuses in dictionary_list
        for job in dictionary_list:
            filename = job['filename']
            if filename in reverse_lookup:
                job['status'] = reverse_lookup[filename]

    logging.info("Finding directories with textfiles: ")
    # Find all directories with the completed.txt file in it.
    logging.info(f'find {project_work_dir} -name completed.txt') 
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name completed.txt')
    output_complete = stdout.read().decode().strip().split('\n')
    output_complete_files = [(completed.replace("mmseg-personal/work_dirs/", "").replace("/completed.txt", ""), "COMPLETED") for completed in output_complete]
    # Find all directories with the error_occurred.txt file in it. 
    logging.info(f'find {project_work_dir} -name error_occurred.txt')
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name error_occurred.txt')
    output_error = stdout.read().decode().strip().split('\n')
    output_error_files = [(error.replace("mmseg-personal/work_dirs/", "").replace("/error_occurred.txt", ""), "ERROR") for error in output_error]
    # Find all directories with the in_progress.txt file in it. 
    logging.info(f'find {project_work_dir} -name in_progress.txt')
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name in_progress.txt')
    output_progress = stdout.read().decode().strip().split('\n')
    output_progress_files = [(progress.replace("mmseg-personal/work_dirs/", "").replace("/in_progress.txt", ""), "RUNNING") for progress in output_progress]
    # Find all directories with the extracted.txt file in it. 
    logging.info(f'find {project_work_dir} -name extracted.txt')
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name extracted.txt')
    output_extracted = stdout.read().decode().strip().split('\n')
    output_extracted_files = [(extracted.replace("mmseg-personal/work_dirs/", "").replace("/extracted.txt", ""), "FINISHED") for extracted in output_extracted]

    # Add/update status of files in json based off of which text file is found within the directories
    for completed in output_complete_files:
        completed_working_directory, completed_status = completed

        for entry in dictionary_list:
            if entry["working_directory"] == completed_working_directory:
                if entry["status"] != completed_status:
                    logging.info(f"Changing {entry['job_name']} status from {entry['status']} to {completed_status}")
                    print(f"Changing {entry['job_name']} status from {entry['status']} to {completed_status}")
                    entry["status"] = completed_status
                    with open(cfg.json_file_path, 'w') as json_file:
                        json.dump(dictionary_list, json_file, indent=4)
                break

    for error in output_error_files:
        error_working_directory, error_status = error

        for entry in dictionary_list:
            if entry["working_directory"] == error_working_directory:
                if entry["status"] != error_status:
                    logging.info(f"Changing {entry['job_name']} status from {entry['status']} to {error_status}")
                    print(f"Changing {entry['job_name']} status from {entry['status']} to {error_status}")
                    entry["status"] = error_status
                    with open(cfg.json_file_path, 'w') as json_file:
                        json.dump(dictionary_list, json_file, indent=4)
                break

    for progress in output_progress_files:
        progress_working_directory, progress_status = progress

        for entry in dictionary_list:
            if entry["working_directory"] == progress_working_directory:
                if entry["status"] != progress_status:
                    logging.info(f"Changing {entry['job_name']} status from {entry['status']} to {progress_status}")
                    print(f"Changing {entry['job_name']} status from {entry['status']} to {progress_status}")
                    entry["status"] = progress_status
                    with open(cfg.json_file_path, 'w') as json_file:
                        json.dump(dictionary_list, json_file, indent=4)
                break

    for extracted in output_extracted_files:
        extracted_working_directory, extracted_status = extracted

        for entry in dictionary_list:
            if entry["working_directory"] == extracted_working_directory:
                if entry["status"] != extracted_status:
                    logging.info(f"Changing {entry['job_name']} status from {entry['status']} to {extracted_status}")
                    print(f"Changing {entry['job_name']} status from {entry['status']} to {extracted_status}")
                    entry["status"] = extracted_status
                    with open(cfg.json_file_path, 'w') as json_file:
                        json.dump(dictionary_list, json_file, indent=4)
                break

    # if len(output_complete_files) == 0 and len(output_error_files) == 0 and len(output_progress_files) == 0:
    #     print("No jobs are currently running")
    #     logging.info("No jobs are currently running")

    status_counter = {}
    for item in dictionary_list:
        status = item["status"]
        if status in status_counter:
            status_counter[status] += 1
        else:
            status_counter[status] = 1

        
    print_green("Updated batch_files.json")
    
    return status_counter.get('FINISHED', 0), status_counter.get('COMPLETED', 0), status_counter.get('ERROR', 0), status_counter.get('RUNNING', 0), status_counter.get('QUEUED', 0)

def update_json_new(ssh):
    
    # Load the existing JSON file
    if os.path.exists(cfg.json_file_path):
        logging.info(f"Updating Json file found at: {cfg.json_file_path}")
        with open(cfg.json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
            logging.info(f"Found json file entries: {dictionary_list}")
    else:
        logging.info("Creating new JSON file.")
        dictionary_list = []
    
    folder_directory = os.path.join(cfg.REMOTE_WORKING_PROJECT, *cfg.REMOTE_BATCH_FILE_LOCATION.split('/')[:-1]).replace("\\", "/")
    print_blue(f"- Updating JSON file: {cfg.json_file_path} -")
    # Handle _QUEUED directory
    queued_directory = os.path.join(folder_directory, '_QUEUED').replace("\\", "/")
    queued_files = rops.list_remote_files(ssh, queued_directory)
    print(f"Queued Files: {queued_files}")
    for batch_file in queued_files:

        for job in dictionary_list:
            if job['filename'] == batch_file:
                job['status'] = 'QUEUED'
                break
        else:
            stdin, stdout, stderr = ssh.exec_command(f"cat {queued_directory}/{batch_file}")
            job_name = ""
            working_directory = ""
            if stderr:
                print_red("Error in updating JSON:" + str(stderr.read().decode().strip()))
                logging.error(f"Error in updating JSON:" + str(stderr.read().decode().strip()))
            for line in stdout:
                line = line.strip()
                if "#SBATCH --job-name=" in line:
                    job_name = line.replace('#SBATCH --job-name=', '').replace(' ', '')

                if "python3 ~/mmseg-personal/tools/train.py" in line:
                    working_line = line.replace('python3 ~/mmseg-personal/tools/train.py ~', '').replace('.py', '').split("/")
                    working_length = len(working_line)
                    working_directory = working_line[working_length - 1]
            new_job = {
                "filename": batch_file,
                "job_name": job_name,
                "working_directory": working_directory,
                "status": "QUEUED"
            }
            dictionary_list.append(new_job)

    # Handle _RUNNING directory
    running_directrory = os.path.join(folder_directory, '_RUNNING').replace("\\", "/")
    running_files = rops.list_remote_files(ssh, running_directrory)
    print(running_files)
    for batch_file in running_files:
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(running_directrory, batch_file).replace("\\", "/"))
        print(job_name)
        jobs = rops.get_squeue_jobs(ssh)

        if job_name in jobs:
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'RUNNING'
                    break
        else:
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job = rops.check_and_update_status(ssh, job, 'in_progress.txt', 'ERROR',
                                                  os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                  os.path.join(folder_directory, '_ERROR').replace("\\", "/"))
                    job = rops.check_and_update_status(ssh, job, 'completed.txt', 'COMPLETED',
                                                  os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                  os.path.join(folder_directory, '_COMPLETED').replace("\\", "/"))
                    job = rops.check_and_update_status(ssh, job, 'extracted.txt', 'FINISHED',
                                                  os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                  os.path.join(folder_directory, '_FINISHED').replace("\\", "/"))
                    
                    break

    # Handle _ERROR directory
    error_directory = os.path.join(folder_directory, '_ERROR').replace("\\", "/")
    error_files = rops.list_remote_files(ssh, error_directory)
    print(error_files)
    for batch_file in error_files:
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(error_directory, batch_file).replace("\\", "/"))
        print(job_name)
        work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job_name).replace("\\", "/")
        command = f"find {work_dir} -maxdepth 1 -name error_occurred.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file and job['status'] != 'QUEUED':
                    job['status'] = 'ERROR'
                    break

    # Handle _COMPLETED directory
    completed_directory = os.path.join(folder_directory, '_COMPLETED').replace("\\", "/")
    completed_files = rops.list_remote_files(ssh, completed_directory)
    print(completed_files)
    for batch_file in completed_files:
        print(batch_file)
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(completed_directory, batch_file).replace("\\", "/"))
        print(job_name)
        work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job_name)
        command = f"find {work_dir} -maxdepth 1 -name completed.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'COMPLETED'
                    break

    # Handle _FINISHED directory
    finished_directory = os.path.join(folder_directory, '_FINISHED').replace("\\", "/")
    finished_files = rops.list_remote_files(ssh, finished_directory)
    for batch_file in finished_files:
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(finished_directory,batch_file).replace("\\", "/"))
        work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job_name).replace("\\", "/")
        command = f"find {work_dir} -maxdepth 1 -name extracted.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'FINISHED'
                    break

    # Write back the updated JSON file
    with open(cfg.json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)

    logging.info("Updated batch_files.json")
    
    status_counter = {}
    for item in dictionary_list:
        status = item["status"]
        if status in status_counter:
            status_counter[status] += 1
        else:
            status_counter[status] = 1

    return status_counter.get('FINISHED', 0), status_counter.get('COMPLETED', 0), status_counter.get('ERROR', 0), status_counter.get('RUNNING', 0), status_counter.get('QUEUED', 0)

def find_sbatch_files_from_json():
    logging.info("Entering find_sbatch_files_from_json()")
    # Find and return batch files shown in json_file 
    if os.path.exists(cfg.json_file_path):
        with open(cfg.json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)

            # populate list of files that are found in the json file
        existing_filenames = {entry['filename'] for entry in dictionary_list}
        logging.info(f"Files that are found and added to the JSON file: {existing_filenames}")
        return existing_filenames
    logging.info("JSON file is not found")
    return None