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
    #cfg.json_file_path = 'batch_files.json'
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
    for batch_file in running_files:
        # job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(running_directrory, batch_file).replace("\\", "/"))
        job_name = rops.get_job_name_from_batch_file(ssh, os.path.join(running_directrory, batch_file).replace("\\", "/"))
        jobs = rops.get_squeue_jobs(ssh)
        if jobs: 
            for job in jobs: 
                if job['name'] == job_name:
                    for json_job in dictionary_list:
                        if json_job['filename'] == batch_file:
                            job['status'] = 'RUNNING'
                            break
                else:
                    for job in dictionary_list:
                        if job['filename'] == batch_file:
                            # Check for inprogress.txt 
                            job = rops.check_and_update_status(ssh, job, 'in_progress.txt', 'ERROR',
                                                        os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                        os.path.join(folder_directory, '_ERROR').replace("\\", "/"))
                            # Check for completed.txt 
                            job = rops.check_and_update_status(ssh, job, 'completed.txt', 'COMPLETED',
                                                        os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                        os.path.join(folder_directory, '_COMPLETED').replace("\\", "/"))
                            # Check for extracted.txt 
                            job = rops.check_and_update_status(ssh, job, 'extracted.txt', 'FINISHED',
                                                        os.path.join(folder_directory, '_RUNNING').replace("\\", "/"),
                                                        os.path.join(folder_directory, '_FINISHED').replace("\\", "/"))
                            
                            break

    # Handle _ERROR directory
    error_directory = os.path.join(folder_directory, '_ERROR').replace("\\", "/")
    error_files = rops.list_remote_files(ssh, error_directory)
    print(f"Error files: {error_files}")
    for batch_file in error_files:
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(error_directory, batch_file).replace("\\", "/"))
        work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job_name).replace("\\", "/")
        command = f"find {work_dir} -maxdepth 1 -name error_occurred.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file and job['status'] != 'QUEUED':
                    job['status'] = 'ERROR'
                    break
        if stderr.read().strip():
            print_red(f"Error finding error_occurred.txt in {work_dir}")
            logging.error(f"Error finding error_occurred.txt in {work_dir}")

        command = f"find {work_dir} -maxdepth 1 -name in_progress.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'RUNNING'
                    break

        command = f"find {work_dir} -maxdepth 1 -name completed.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'COMPLETED'
                    break

        command = f"find {work_dir} -maxdepth 1 -name extracted.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'FINISHED'
                    break
        if stderr.read().strip():
            print_red(f"Error finding completed.txt in {work_dir}")
            logging.error(f"Error finding completed.txt in {work_dir}")

    # Handle _COMPLETED directory
    completed_directory = os.path.join(folder_directory, '_COMPLETED').replace("\\", "/")
    completed_files = rops.list_remote_files(ssh, completed_directory)
    print(f"Completed Files: {completed_files}")
    for batch_file in completed_files:
        job_name = rops.get_python_file_name_from_batch_file(ssh, os.path.join(completed_directory, batch_file).replace("\\", "/"))
        work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job_name)
        command = f"find {work_dir} -maxdepth 1 -name completed.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'COMPLETED'
                    break
        command = f"find {work_dir} -maxdepth 1 -name extracted.txt"
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.read().strip():
            for job in dictionary_list:
                if job['filename'] == batch_file:
                    job['status'] = 'FINISHED'
                    break
        if stderr.read().strip():
            print_red(f"Error finding completed.txt in {work_dir}")
            logging.error(f"Error finding completed.txt in {work_dir}")

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
    print(status_counter)
    return status_counter.get('FINISHED', 0), status_counter.get('COMPLETED', 0), status_counter.get('ERROR', 0), status_counter.get('RUNNING', 0), status_counter.get('QUEUED', 0)


def set_status_of_batch_file(status, batch_file='', job_name='', working_directory=''):
    """
    Sets the status of a job based on the provided batch_file, job_name, or working_directory.

    :param status: New status to set.
    :param batch_file: (optional) The filename of the batch file to identify the job.
    :param job_name: (optional) The job name to identify the job.
    :param working_directory: (optional) The working directory to identify the job.
    """
    # Read the JSON file if it exists
    if os.path.exists(cfg.json_file_path):
        with open(cfg.json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    else:
        raise FileNotFoundError(f"JSON file not found at path: {cfg.json_file_path}")

    # Update the status for the first match found based on the provided identifiers
    job_found = False
    for job in dictionary_list:
        if batch_file and job["filename"] == batch_file:
            job["status"] = status
            job_found = True
            break
        elif job_name and job["job_name"] == job_name:
            job["status"] = status
            job_found = True
            break
        elif working_directory and job["working_directory"] == working_directory:
            job["status"] = status
            job_found = True
            break

    # Write back to the JSON file if a job was found and updated
    if job_found:
        logging.info(f"Updated {job['filename']} to status {job['status']}")
        with open(cfg.json_file_path, 'w') as json_file:
            json.dump(dictionary_list, json_file, indent=4)
    else:
        print_red(f"No job found with the given identifiers: batch_file='{batch_file}', job_name='{job_name}', working_directory='{working_directory}'.")
        logging.error(f"No job found with the given identifiers: batch_file='{batch_file}', job_name='{job_name}', working_directory='{working_directory}'. "\
                      f"\nFile name is not found in {cfg.json_file_path} ")


def find_sbatch_files_from_json():
    # NOT USED
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