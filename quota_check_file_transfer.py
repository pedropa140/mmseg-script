import paramiko
import logging
import time
import os
import re
import subprocess
from dotenv import load_dotenv
import schedule
import json
import traceback
from collections import defaultdict

'''
To make use of the dotenv() command, create a new file labelled ".env" and fill in the blanks as needed:
netid=[username]
remote_host=ilab4.cs.rutgers.edu
password=[password_to_remote_host]
local_path=[/path/to/place/folders/on_local_pc]
completed_marker_file=completed.txt
finished_marker_file=finished.txt
remote_base_path=/common/home/bn155
remote_working_project=mmseg-personal
remote_work_dir=work_dirs
remote_batch_file_location=tools/batch_files/_QUEUED
plink_path=[None]
pscp_path=[None]

USAGE:
Make sure you have your .env file with the variables mentioned above

To use this script, we would need the mmseg-personal (or mmselfsup-personal), batch files located in 
tools/batch_files/ with five folders within there named _QUEUED, _COMPLETED, _ERROR, _RUNNING, _FINISHED.

if running on windows, you may need to download putty and find the location of the plink.exe and pscp.exe 
files and add those paths into the plink_path and pscp_path. Easier option though is to use WSL on windows 
or a linux system. 

Make sure you have sshpass installed using sudo apt-get install sshpass on linux or 
sudo apt get install sshpass on WSL. Make sure you have the requirements installed using 
pip install -r requirements.txt

run this script on a local pc using:
    python quota_check_file_transfer.py
'''

load_dotenv()

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

PLINK_PATH=os.getenv('plink_path')
PSCP_PATH=os.getenv('pscp_path')

if PLINK_PATH != '' and PSCP_PATH != '':
    print(f"Found path to plink.exe: {PLINK_PATH}")
    print(f"Found path to pscp.exe: {PSCP_PATH}")
    print("Assuming Operating system to be windows!")
    windows=True
    linux=False

else:
    print(f"Did not find plink.exe or pscp.exe paths")
    print("Assuming Operating System to be Linux")
    windows=False
    linux=True


# Configuration
REMOTE_HOST = 'ilab4.cs.rutgers.edu'
if REMOTE_HOST is None:
    print("Remote Host not found. Did you create a .env file?")
    logging.error("Remote Host not found. Did you create a .env file?")
    
USERNAME = os.getenv('netid')
if USERNAME is None:
    print("Username not found. Did you create a .env file?")
    logging.error("Username not found. Did you create a .env file?")

LOCAL_PATH = os.getenv('local_path')

if LOCAL_PATH is None:
    print("Local path not found. Check path and .env file")
if os.path.exists(LOCAL_PATH):
    print(f"Local path found!: {LOCAL_PATH}")
else:
    print("The local path provided does not exist. Please check your path before proceeding.")
    logging.error("The local path provided does not exist. Please check your path before proceeding.")
    
PASSWORD = os.getenv('password')

THRESHOLD = 45  # Set your threshold percentage
FINISHED_MARKER_FILE = 'extracted.txt'  # The file that indicates the directory should be moved
COMPLETED_MARKER_FILE = 'completed.txt' # Indicates that files are done training, and log_extraction can be run
# LOCAL_PATH = '/home/diez-lab/Corrosion_Detection/'

REMOTE_BASE_PATH = '/common/home/bn155'
REMOTE_WORKING_PROJECT = 'mmseg-personal'
REMOTE_WORK_DIR = 'work_dirs'
REMOTE_BATCH_FILE_LOCATION = 'tools/batch_files/_QUEUED'
REMOTE_BATCH_FILE_PATH = 'mmseg-personal/tools/batch_files/_QUEUED'

last_status_counts = None
json_file_path = 'batch_files.json'
job_threshold = 3
queued_jobs = []


def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_red(text):
    print(f"\033[91m{text}\033[0m")

def print_blue(text):
    print(f"\033[38;2;50;128;128m{text}\033[0m")

# COMPLETED
def connect_ssh():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # PASSWORD = getpass("Enter your SSH password: ")
    try:
        ssh.connect(hostname=REMOTE_HOST, username=USERNAME, password=PASSWORD)
        print_green("Successfully connected to SSH.")
        logging.info("Successfully connected to SSH.")
        return ssh
    except Exception as e:
        print_red(f"Failed to connect to SSH: {e}")
        logging.error(f"Failed to connect to SSH: {e}")
        return None
# COMPLETED
def check_storage_usage(ssh):
    logging.info("check_storage_usage(ssh) ")
    # Command run to get the storage used by the user
    stdin, stdout, stderr = ssh.exec_command('quota -vs')
    output = stdout.read().decode()
    
    # Parse the output to get the percentage used
    lines = output.splitlines()  # Split the output into lines

    for i, line in enumerate(lines):
        if 'communis.lcsr.rutgers.edu:/common/home' in line:
            # Move to the next line after the matched line
            next_line = lines[i + 1]

            # Extract space and quota (assume it's the first and second items in the next line)
            match = re.search(r'(\d+M)\s+(\d+M)', next_line)
            if match:
                used_space_str = match.group(1)
                total_quota_str = match.group(2)

                # Convert these to integers
                used_space = int(used_space_str.replace("M", ""))
                total_quota = int(total_quota_str.replace("M", ""))

                # Calculate the usage percentage
                usage_percentage = (used_space / total_quota) * 100

                print_green(f"Usage Percentage: {usage_percentage:.2f}%")
                logging.info(f"Usage Percentage: {usage_percentage:.2f}%")
                return usage_percentage

    print_red("Could not determine storage usage.")
    logging.error("Could not determine storage usage.")
    return None  # If the line wasn't found
# COMPLETED
def find_directories_to_move(ssh):
    logging.info("find_directories_to_move()")
    # Find directories to move by checking for text file that says "extracted.txt"
    directories_to_move = []
    project_work_dir = os.path.join(REMOTE_BASE_PATH, REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR).replace("\\", "/")
    logging.info(f"Executing: find {project_work_dir} -name {FINISHED_MARKER_FILE}")
    
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {FINISHED_MARKER_FILE}')
    
    stderr_output = stderr.read().decode().strip()
    if stderr_output:
        logging.error(stderr_output.split('\n'))
        print_red(stderr_output.split('\n'))
    
    output = stdout.read().decode().strip().split('\n')
    
    for line in output:
        if line:  # Make sure it's not an empty line
            directory = os.path.dirname(line)
            directories_to_move.append(directory)
            print_green(f"Found directory to move: {directory}")
            logging.info(f"Found directory to move: {directory}")
    
    return directories_to_move
# COMPLETED
def move_directories(ssh, directories):
    logging.info(f" move_directories({directories})")
    # This method is used to sync file contents from remote to local pc. It then removes after files have been synced
    for directory in directories:
        try:
            # Use subprocess to run rsync and capture output
            # COMMAND FOR LINUX PC
            if linux:
                logging.info(f"Executing: rsync -avz '{USERNAME}@{REMOTE_HOST}:{directory}', {LOCAL_PATH}")
                print_red(f"Executing: rsync -avz '{USERNAME}@{REMOTE_HOST}:{directory}', {LOCAL_PATH}")
                # UNCOMMENT SSHPASS LINE IF RUNNING ON LAB PC                
                command = [
                #    'sshpass', '-p', PASSWORD,
                    'rsync', '-avz',
                    f'{USERNAME}@{REMOTE_HOST}:{directory}', LOCAL_PATH
                ]
                result_rsync = subprocess.run(command, capture_output=True, text=True)

                # Check if rsync was successful                
                if result_rsync.returncode != 0:
                    logging.error(f"rsync failed with error: {result_rsync.stderr}")
                    raise Exception(f"rsync failed with error: {result_rsync.stderr}")
                
            if windows:
                # # Construct the plink command to move the directory on the remote machine (e.g., using mv or scp)
                # # If you're moving the directory remotely, you might want to use mv command.
                # remote_command = f"mv {directory} {LOCAL_PATH}"

                # # Run the plink command to execute the move on the remote server
                # plink_cmd = [PLINK_PATH, "-pw", PASSWORD, f"{USERNAME}@{REMOTE_HOST}", remote_command]
                # result_move = subprocess.run(plink_cmd, capture_output=True, text=True)

                # if result_move.returncode != 0:
                #     logging.error(f"mv command failed with error: {result_move.stderr}")
                #     raise Exception(f"mv {directory} {LOCAL_PATH}  failed with error: {result_move.stderr}")

                # Use pscp to copy the remote directory to the local machine
                pscp_cmd = [PSCP_PATH, "-r", "-pw", PASSWORD, f"{USERNAME}@{REMOTE_HOST}:{directory}", LOCAL_PATH]
                result_scp = subprocess.run(pscp_cmd, capture_output=True, text=True)

                if result_scp.returncode != 0:
                    logging.error(f"scp command failed with error: {result_scp.stderr}")
                    raise Exception(f"scp failed with error: {result_scp.stderr}")

                # # Optionally, remove the remote directory after copying to ensure it was successfully moved
                # remove_command = f"rm -rf {directory}"
                # plink_cmd_remove = [PLINK_PATH, "-pw", PASSWORD, f"{USERNAME}@{REMOTE_HOST}", remove_command]
                # result_remove = subprocess.run(plink_cmd_remove, capture_output=True, text=True)  

                # if result_remove.returncode != 0:
                #     logging.error(f"mv command failed with error: {result_remove.stderr}")
                #     raise Exception(f"rsync failed with error: {result_remove.stderr}")

            print_green(f"Moved directory {directory} to local machine: {LOCAL_PATH}")
            logging.info(f"Moved directory {directory} to local machine: {LOCAL_PATH}")
            
            # Remove the directory on the remote machine after successful transfer
            logging.info(f"Executing: rm -rf {directory}")
            print_red(f"Executing (but not really): rm -rf {directory}")
            # stdin, stdout, stderr = ssh.exec_command(f'rm -rf {directory}')
            # error = stderr.read().decode().strip()
            # if error:
            #     raise Exception(f"Failed to remove directory {directory} on remote machine: {error}")

            print_green(f"Removed directory {directory} from remote machine.")
            logging.info(f"Removed directory {directory} from remote machine.")
        
        except Exception as e:
            print_red(f"Error processing directory {directory}: {e}")
            logging.error(f"Error processing directory {directory}: {e}")
# COMPLETED
def create_json(ssh):
    base_dir = '/'.join(os.path.join(REMOTE_WORKING_PROJECT, REMOTE_BATCH_FILE_LOCATION).replace("\\", "/").split('/')[:-1])
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
    json_file_path = 'batch_files.json'
    logging.info(f"create_json(): Comparing batch files found in {base_dir} and {json_file_path}")
    if os.path.exists(json_file_path):
        logging.info(f"Found a premade JSON file for batch_files at {json_file_path}")
        with open(json_file_path, 'r') as json_file:
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
    
    with open(json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)
# COMPLETED
def update_json(ssh):
    logging.info("Updating Json file")
    # TODO REMOVE FILES FORM JSON FILE IF BATCH FILES ARE MOVED/NOT PRESENT
    dictionary_list = []
    folder_list = []
    directory_dictionary = {}
    project_work_dir = f'{REMOTE_WORKING_PROJECT}/{REMOTE_WORK_DIR}'
    folder_directory = os.path.join(REMOTE_WORKING_PROJECT, *REMOTE_BATCH_FILE_LOCATION.split('/')[:-1]).replace("\\", "/")

    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
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
                    with open(json_file_path, 'w') as json_file:
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
                    with open(json_file_path, 'w') as json_file:
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
                    with open(json_file_path, 'w') as json_file:
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
                    with open(json_file_path, 'w') as json_file:
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

def update_json_wrapper(ssh):
    global last_status_counts
    last_status_counts = update_json(ssh)
# COMPLETED
def get_squeue_jobs(ssh):
    logging.info("Running Squeue to see which jobs are running")
    command = 'squeue --format="%.18i %.9P %.30j %.8u %.8T %.10M %.9l %.6D %R" --me'
    stdin, stdout, stderr = ssh.exec_command(command)
    squeue_output = stdout.read().decode().strip().splitlines()
    jobs = []
    for line in squeue_output[1:]:  # Skip header line
        parts = line.split()
        job_id, name, state = parts[0], parts[2], parts[4]
        jobs.append({"job_id": job_id, "name": name, "state": state})
    logging.info(f"Jobs found to be running: {jobs}")
    return jobs
# COMPLETED  
def check_batch_files(ssh, jobs):
    base_dir = os.path.join(REMOTE_WORKING_PROJECT, REMOTE_BATCH_FILE_LOCATION).replace("\\", "/")
    base_dir = '/'.join(base_dir.split('/')[:-1])  # Remove the last part (_QUEUED) to get the main directory
    
    dirs_to_check = [d for d in list_remote_directories(ssh, base_dir) if d.startswith('_')]
    logging.info(f"Checking batch_files in {base_dir} in these directories: {dirs_to_check}")
    for dir_name in dirs_to_check:
        full_dir_path = os.path.join(base_dir, dir_name).replace("\\", "/")
        for filename in list_remote_files(ssh, full_dir_path):
            batch_file_path = os.path.join(full_dir_path, filename).replace("\\", "/")
            job_name = get_job_name_from_batch_file(ssh, batch_file_path)

            matching_job = next((job for job in jobs if job["name"] == job_name), None)
            if matching_job:
                if dir_name != "_RUNNING":
                    move_batch_file(ssh, batch_file_path, os.path.join(base_dir, "_RUNNING").replace("\\", "/"))
            else:
                check_and_handle_non_running_job(ssh, job_name, batch_file_path, base_dir)

    # Additional step: Handle jobs that are no longer in squeue
    handle_cancelled_jobs(ssh, jobs, base_dir)
# COMPLETED
def get_job_name_from_batch_file(ssh, batch_file_path):
    logging.info(f"Executing: cat {batch_file_path}")
    stdin, stdout, stderr = ssh.exec_command(f'cat {batch_file_path}')
    for line in stdout:
        if line.startswith("#SBATCH --job-name="):
            logging.info(f"Found job-name: ({line.split('=')[-1].strip()}) from {batch_file_path}")
            return line.split("=")[-1].strip()
    return None
# COMPLETED
def move_batch_file(ssh, src, dest_dir):
    logging.info(f"Executing: mv {src}, {dest_dir})")
    command = f"mv {src} {dest_dir}/"
    stdin, stdout, stderr = ssh.exec_command(command)
    error = stderr.read().decode().strip()
    if error:
        logging.error(f"Error moving batch file {src}: {error}")
        print_red(f"Error moving batch file {src}: {error}")
    else:
        logging.info(f"Moved {src} to {dest_dir}")
        print(f"Moved {src} to {dest_dir}")
# COMPLETED
def check_and_handle_non_running_job(ssh, job_name, batch_file_path, base_dir):
    logging.info(f"Check and handle non running jobs: job-name : {job_name}, batch_file_path : {batch_file_path} in directory {base_dir})")
    python_file_name = get_python_file_name_from_batch_file(ssh, batch_file_path)
    work_dir_path = os.path.join(REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR, python_file_name).replace("\\", "/")
    in_progress_file = os.path.join(work_dir_path, "in_progress.txt").replace("\\", "/")
    error_file = os.path.join(work_dir_path, "error_occurred.txt").replace("\\", "/")

    if check_remote_file_exists(ssh, in_progress_file):
        rename_remote_file(ssh, in_progress_file, error_file)
        move_batch_file(ssh, batch_file_path, os.path.join(base_dir, "_ERROR").replace("\\", "/"))
# COMPLETED
def handle_cancelled_jobs(ssh, jobs, base_dir):
    logging.info(f"Handle cancelled jobs: Jobs: {jobs}, in directory: {base_dir}) ---")
    # Check all work_dirs for in_progress.txt files and handle those that are no longer running
    work_dirs = list_remote_directories(ssh, os.path.join(REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR).replace("\\", "/"))
    for work_dir in work_dirs:
        work_dir_path = os.path.join(REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR, work_dir).replace("\\", "/")
        in_progress_file = os.path.join(work_dir_path, "in_progress.txt").replace("\\", "/")
        error_file = os.path.join(work_dir_path, "error_occurred.txt").replace("\\", "/")

        if check_remote_file_exists(ssh, in_progress_file):
            # Extract the job name from the batch file associated with this work_dir
            batch_file_name = find_associated_batch_file(ssh, base_dir, work_dir)
            logging.info(f"Handling cancelled job: {batch_file_name}")
            print(f"Handling cancelled job: {batch_file_name}")
            if batch_file_name:
                job_name = get_job_name_from_batch_file(ssh, batch_file_name)
                matching_job = next((job for job in jobs if job["name"] == job_name), None)
                if not matching_job:
                    # Job is no longer running, so mark as error and move batch file to _ERROR
                    rename_remote_file(ssh, in_progress_file, error_file)
                    move_batch_file(ssh, batch_file_name, os.path.join(base_dir, "_ERROR").replace("\\", "/"))
# COMPLETED
def find_associated_batch_file(ssh, base_dir, work_dir):
    logging.info(f"Find associated batch file: {base_dir}, {work_dir})")
    # Find the batch file associated with the work_dir
    for dir_name in list_remote_directories(ssh, base_dir):
        full_dir_path = os.path.join(base_dir, dir_name).replace("\\", "/")
        for filename in list_remote_files(ssh, full_dir_path):
            batch_file_path = os.path.join(full_dir_path, filename).replace("\\", "/")
            python_file_name = get_python_file_name_from_batch_file(ssh, batch_file_path)
            if python_file_name == work_dir:
                logging.info(f"Found batch file: {batch_file_path}")
                return batch_file_path
    return None
# COMPLETED
def get_python_file_name_from_batch_file(ssh, batch_file_path):
    logging.info(f"Get folder name from batch file: {batch_file_path})")
    stdin, stdout, stderr = ssh.exec_command(f'cat {batch_file_path}')
    
    for line in stdout:
        line = line.strip()
        if "python3 ~/mmseg-personal/tools/train.py" in line:
            working_line = line.replace('python3 ~/mmseg-personal/tools/train.py ~', '').replace('.py', '').split("/")
            working_length = len(working_line)
            working_directory = working_line[working_length - 1]
            logging.info(f"Found name of folder in batch file: {working_directory}")
            return working_directory
    return None
# COMPLETED
def list_remote_directories(ssh, path):
    logging.info(f"list remote directories in: {path})")
    logging.info(f"Executing: 'ls -d {path}/*/'")
    stdin, stdout, stderr = ssh.exec_command(f'ls -d {path}/*/')
    dirs = stdout.read().decode().strip().splitlines()
    return [os.path.basename(d.rstrip('/')) for d in dirs]
# COMPLETED
def list_remote_files(ssh, path):
    logging.info(f"list remote files in: {path})")
    logging.info(f"Executing command: 'ls {path}'")
    stdin, stdout, stderr = ssh.exec_command(f'ls {path}')
    files = stdout.read().decode().strip().splitlines()
    logging.info(f"Files found: {files}")
    return files
# COMPLETED
def check_remote_file_exists(ssh, path):
    logging.info(f"Check if remote file exists in: {path})")
    logging.info(f"Executing: 'if [ -f {path} ]; then echo 'exists'; fi'")
    stdin, stdout, stderr = ssh.exec_command(f'if [ -f {path} ]; then echo "exists"; fi')
    result = stdout.read().decode().strip()
    # print(f"{path}: {result}")
    logging.info(f"{path}: {result}")
    return result == "exists"
# COMPLETED
def rename_remote_file(ssh, src, dest):
    logging.info(f"Rename remote file from:{src} to:{dest})")
    logging.info(f"mv {src} {dest}")
    print(f"Moving file from {src} to {dest}")
    command = f"mv {src} {dest}"
    stdin, stdout, stderr = ssh.exec_command(command)
    error = stderr.read().decode().strip()
    if error:
        logging.error(f"Error renaming {src}: {error}")
        print_red(f"Error renaming {src}: {error}")
    else:
        logging.info(f"Renamed {src} to {dest}")
        print_green(f"Renamed {src} to {dest}")
# COMPLETED
def find_sbatch_files_from_directory(ssh):
    logging.info("Finding sbatch files from directories")
    # Find all the batch files within the specified directory from home directory
    logging.info(f"Executing: 'find {REMOTE_BATCH_FILE_PATH} -name '*.batch''")
    stdin, stdout, stderr = ssh.exec_command(f'find {REMOTE_BATCH_FILE_PATH} -name "*.batch"')
    output = stdout.read().decode()
    sbatch_files = output.splitlines()

    # Split and process all the batch files to add them to a list
    for counter, file in enumerate(sbatch_files):
        # Split the path by "/"
        file_parts = file.split('/')
        # Rejoin the parts without the first directory (Working project diredctory)
        sbatch_files[counter]= '/'.join(file_parts[4:])
    # List of string of all batch files to run from {WORKING_PROJECT} directory
    logging.info(f"Batch files that have not been started: {sbatch_files}")
    return sbatch_files
# COMPLETED
def find_sbatch_files_from_json():
    logging.info("Entering find_sbatch_files_from_json()")
    # Find and return batch files shown in json_file 
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)

            # populate list of files that are found in the json file
        existing_filenames = {entry['filename'] for entry in dictionary_list}
        logging.info(f"Files that are found and added to the JSON file: {existing_filenames}")
        return existing_filenames
    logging.info("JSON file is not found")
    return None
# COMPLETED
def run_sbatch(ssh):
    batch_files_from_directory = find_sbatch_files_from_directory(ssh)
    batch_files_from_json = list(find_sbatch_files_from_json())
    logging.info(f'batch_files_from_directory:\t{batch_files_from_directory}')
    print(f'batch_files_from_directory:\t{batch_files_from_directory}')
    logging.info(f"batch_files_from_json:\t{batch_files_from_json}")
    print(f"batch_files_from_json:\t{batch_files_from_json}")
    # Check json file
    # Find jobs to run by making sure we aren't rerunning already running jobs
        # compare squeue names with Json names and batch_file_directory names
        # find list of exclusive batchfiles, create a list, run the first one on list
        # move executed job into a new directory?

    global queued_jobs
    dictionary_list = []
    json_file_path = 'batch_files.json'

    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    running_item = ""
    for item in dictionary_list:
        if item['status'] == 'QUEUED':
            filename = item['filename']
            job_name = item['job_name']
            
            job_tuple = (filename, job_name)
            queued_jobs.append(job_tuple)
    if len(queued_jobs) > 0:
        running_item = queued_jobs[0][0]
        print(f'QUEUED LIST: {queued_jobs}')
        logging.info(f'QUEUED LIST: {queued_jobs}')
        print(f'QUEUED JOBS: {running_item}')
        logging.info(f'QUEUED JOBS: {running_item}')
        logging.info(f"Executing command: cd {REMOTE_WORKING_PROJECT} ; sbatch {REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}")
        stdin, stdout, stderr = ssh.exec_command(f'cd {REMOTE_WORKING_PROJECT} ; sbatch {REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
        for counter, line in enumerate(stderr):
            print_red(f"Line 193: {line.strip()}")
        for counter, line in enumerate(stdout):
            print_green(f"Line 193: {line.strip()}")
        # PROBLEM OCCURING HERE 
        if (running_item, queued_jobs[0][1]) in queued_jobs:
            queued_jobs.remove((running_item, queued_jobs[0][1]))
        update_json_wrapper(ssh)
    else:
        print("No jobs with status QUEUED")
        logging.info("No jobs with status QUEUED")
# COMPLETED
def move_batch_files_based_on_status(ssh):
    logging.info("Move batch files to their folders based off status in json file")
    # Load JSON data
    with open(json_file_path, 'r') as f:
        batch_files_data = json.load(f)

    for batch_file in batch_files_data:
        filename = batch_file['filename']
        status = batch_file['status']
        base_dir = os.path.join(REMOTE_WORKING_PROJECT, REMOTE_BATCH_FILE_LOCATION).replace("\\", "/")
        base_dir = '/'.join(base_dir.split('/')[:-1])
        
        # Define source and destination directories
        source_dir = f"{base_dir}/_QUEUED/{filename}"
        if status == "RUNNING":
            dest_dir = f"{base_dir}/_RUNNING/{filename}"
            need_to_move = True
        elif status == "ERROR":
            dest_dir = f"{base_dir}/_ERROR/{filename}"
            need_to_move = True
        elif status == "COMPLETED":
            dest_dir = f"{base_dir}/_COMPLETED/{filename}"
            need_to_move = True
        elif status == "FINISHED":
            dest_dir = f"{base_dir}/_FINISHED/{filename}"
            need_to_move = True
        elif status == "QUEUED":
            need_to_move = False
        else:
            print(f"Unknown status '{status}' for file {filename}. Skipping.")
            logging.info(f"Unknown status '{status}' for file {filename}. Skipping.")
            continue
        if need_to_move:
            # Move the file using SSH
            if os.path.exists(source_dir):
                move_command = f"mv {source_dir} {dest_dir}"
                stdin, stdout, stderr = ssh.exec_command(move_command)

                # Check for errors
                error = stderr.read().decode().strip()
                if error:
                    print(f"Error moving {filename}: {error}")
                    logging.info(f"Error moving {filename}: {error}")
                else:
                    print(f"Successfully moved {filename} to {dest_dir}")
                    logging.info(f"Successfully moved {filename} to {dest_dir}")

def log_extraction(ssh):
    logging.info("Extracting logs for models that have completed training")
    try:
        # Find directories that have the completed.txt file indicating that training is done
        project_work_dir = f'{REMOTE_WORKING_PROJECT}/{REMOTE_WORK_DIR}'
        # print(f'Project_Work_dir:\t{project_work_dir}')
        logging.info(f'find {project_work_dir} -name {COMPLETED_MARKER_FILE}')
        stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {COMPLETED_MARKER_FILE}')
        output_complete = stdout.read().decode().strip().split('\n')
        logging.info(output_complete)

        # If there are directories found that have completed training, execute this block
        #largest_json_files = []
        if output_complete != ['']:
            for completed_job in output_complete:
                # Remove the last entry in the path (i.e. DIRECTORY_MARKER_FILE)
                directory = '/'.join(completed_job.split('/')[:-1])
                # Find the largest json file, usually means that it has the most entries indicating it is the json file of the training session
                logging.info(f"Finding largest json log file for: {directory}")
                find_largest_json_file = f'find {directory} -type f -name "*.json" -exec ls -s {{}} + | sort -n | tail -n 1 | awk \'{{print $2}}\''
                stdin, stdout, stderr = ssh.exec_command(find_largest_json_file)
                largest_json = stdout.read().decode().strip()
                
                if largest_json:
                    #largest_json_files.append(largest_json)
                    largest_json_trunc = '/'.join(largest_json.split('/')[1:])
                    # print(f'The largest JSON file located in this directory is: {largest_json_trunc}')
                    logging.info(f"Extracting logs from largest json file in {directory}: {largest_json_trunc}")
                    logging.info(f'cd {REMOTE_WORKING_PROJECT} ; python3 research/log_extraction.py --input {largest_json_trunc}')
                    log_extraction_python = f'cd {REMOTE_WORKING_PROJECT} ; python3 research/log_extraction.py --input {largest_json_trunc}'
                    stdin, stdout, stderr = ssh.exec_command(log_extraction_python)
                    # Output the result of the command
                    print(f"Executed command in directory {directory}:")
                    logging.info(stdout.read().decode())
                    print(stdout.read().decode())
                    logging.error(stderr.read().decode())
                    print(stderr.read().decode())
                    extracted_job = completed_job.replace("completed.txt", "extracted.txt")
                    
                    # Command to rename the file
                    logging.info(f'Executing: mv {completed_job} {extracted_job}')
                    rename_command = f'mv {completed_job} {extracted_job}'
                    # Execute the rename command on the remote server
                    stdin, stdout, stderr = ssh.exec_command(rename_command)
                    
                    # Check for errors
                    error = stderr.read().decode().strip()
                    if error:
                        logging.error(f"Error renaming {completed_job}: {error}")
                        print_red(f"Error renaming {completed_job}: {error}")
                    else:
                        logging.info(f"Successfully renamed {completed_job} to {extracted_job}")
                        print_green(f"Successfully renamed {completed_job} to {extracted_job}")

                else:
                    logging.error(f'No JSON files were found in this directory: {directory}')
                    print_red(f'No JSON files were found in this directory: {directory}')
        else:
            logging.error(f"{COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
            print_red(f"{COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        print(f"An error occured: {str(e)}")

def run_every_hour(ssh):
    logging.info("Running storage check and moving files if they're finished")
    # Check how much storage is being used
    # login to remote pc, run quote -vs, and extract the used storage and storage limit    
    # Move files using rsync to local pc
    usage_percentage = check_storage_usage(ssh)
    if usage_percentage:
        print_green(f"Storage usage is at {usage_percentage:.2f}%.")
        logging.info(f"Storage usage is at {usage_percentage:.2f}%.")
    else:
        print_red("Could not determine storage usage.")
        logging.info("Could not determine storage usage.")
    # If storage is above a level, move the completed trained models over to the local pc
    if usage_percentage > THRESHOLD:
        directories = find_directories_to_move(ssh)
        if directories:
            move_directories(ssh, directories)
        else:
            print_red("No directories found to move")
            logging.info("No directories found to move.")
    else:
        print_green("Storage usage is within limits.")
        logging.info("Storage usage is within limits.")

def test_model(ssh):
    # TODO Implement python tools/test.py custom_config.py work_dirs/[job-name]/best_*.pth --eval mIoU --show-dir work_dir/[job-name]/best_*_iter_output function call on completed models
    return NotImplementedError

# def nuke_all_sbatch(ssh):
#     stdin, stdout, stderr = ssh.exec_command(f'squeue --format="%.18i %.9P %.30j %.8u %.8T %.10M %.9l %.6D %R" --me')
#     all_jobs = []
#     for counter, line in enumerate(stdout):
#         if counter == 0:
#             continue
#         print(f'CANCEL ALL SBATCH: jobid: {line.split()[1]}, job-name: {line.split()[2]}, status: {line.split()[4]}')
#         # 2 is job name
#         # 4 is status

def main():
    ssh = connect_ssh()
    create_json(ssh)
    # run_every_hour(ssh)
    # COMPLETED, ERROR, RUNNING, QUEUED = update_json(ssh)
    run_counter = 0
    global last_status_counts
    try:
        while True:
            # ERROR OCCURRING WHERE ERROR_OCCURRED.TXT OVERRIDES THE _QUEUED DIRECTORY AND DOESN'T RUN SBATCH
            # ERROR OCCURRING WITH RUNNING FILES NOT BEING MOVED TO COMPLETED/FINISHED
            update_json_wrapper(ssh)
            print(f'STATUS DICTIONARY:\nFinished = {last_status_counts[0]} \nCompleted = {last_status_counts[1]} \nError = {last_status_counts[2]} \nRunning = {last_status_counts[3]} \nQueued = {last_status_counts[4]}')
            
            # Run sbatch jobs if there are fewer than a certain amount.
            if last_status_counts[3] < job_threshold:
                run_sbatch(ssh)
                move_batch_files_based_on_status(ssh)

            jobs = get_squeue_jobs(ssh)
            check_batch_files(ssh, jobs)
            move_batch_files_based_on_status(ssh)

            # Check storage usage and move directories to local pc if they are finished and storage is above a certain memory threshold
            run_every_hour(ssh)

            # Look and extract logs for jobs that are completed. 
            log_extraction(ssh)

            run_counter+=1
            print_green(f"Program Complete counter: {run_counter}")
            time.sleep(60)

        # TODO COME UP WITH LOGICAL FLOW OF OPERATIONS TO FIND, RUN, EXTRACT DATA AND RELOCATE FILES LOCALLY

        # run_every_hour(ssh)
        # schedule.every().hour.do(run_every_hour, ssh)

        # schedule.every().minute.do(run_every_hour, ssh)
        # schedule.every().minute.do(create_json, ssh)
        # schedule.every().minute.do(update_json_wrapper, ssh)

        # run 3 at a time
        # create json of file names saying if completed or not
        
        # # ------------
        # global last_status_counts
        # update_json_wrapper(ssh)
        
        # log_extraction(ssh)
        # if last_status_counts[3] < 4:
        #     run_sbatch(ssh)
        # print(f'STATUS DICTIONARY: {last_status_counts}')
        # # ----------
        # move_batch_files_based_on_status(ssh)
        # cancel_all_sbatch(ssh)
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)
        
    except Exception as e:
        print(e)
        logging.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()