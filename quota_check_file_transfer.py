import paramiko
import logging
import time
import os
#from getpass import getpass
import re
import subprocess
from dotenv import load_dotenv
import schedule
import json
import traceback
'''
To make use of the dotenv() command, create a new file labelled ".env" and fill in the blanks as needed:
netid=[insert_netid] # No spaces and no need for quotes around name
password=password
local_path=/local/path/to/save/files
remote_path=/path/to/look/for/files
remote_host=domain_to_connect_to
marker_file=filename_to_look_for.txt
'''

load_dotenv()

# Configuration
REMOTE_HOST = 'ilab4.cs.rutgers.edu'
USERNAME = os.getenv('netid')
PASSWORD = os.getenv('password')
THRESHOLD = 45  # Set your threshold percentage
FINISHED_MARKER_FILE = 'extracted.txt'  # The file that indicates the directory should be moved
COMPLETED_MARKER_FILE = 'completed.txt'
LOCAL_PATH = '/home/diez-lab/Corrosion_Detection/'



REMOTE_BASE_PATH = '/common/home/bn155'
REMOTE_WORKING_PROJECT = 'mmseg-personal'
REMOTE_WORK_DIR = 'work_dirs'
REMOTE_BATCH_FILE_LOCATION = 'tools/batch_files/_QUEUED'
REMOTE_BATCH_FILE_PATH = 'mmseg-personal/tools/batch_files/_QUEUED'


last_status_counts = None
json_file_path = 'batch_files.json'
job_threshold = 3
queued_jobs = []
# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_red(text):
    print(f"\033[91m{text}\033[0m")

def connect_ssh():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # PASSWORD = getpass("Enter your SSH password: ")
    try:
        ssh.connect(hostname=REMOTE_HOST, username=USERNAME, password=PASSWORD)
        print_green("Successfully connected to SSH.")
        return ssh
    except Exception as e:
        print_red(f"Failed to connect to SSH: {e}")
        return None

def check_storage_usage(ssh):
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
                return usage_percentage

    print_red("Could not determine storage usage.")
    return None  # If the line wasn't found

def find_directories_to_move(ssh):
    # Find directories to move by checking for text file that says "extracted.txt"
    directories_to_move = []
    project_work_dir = os.path.join(REMOTE_BASE_PATH, REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR)
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {FINISHED_MARKER_FILE}')
    output = stdout.read().decode().strip().split('\n')
    for line in output:
        if line:  # Make sure it's not an empty line
            directory = os.path.dirname(line)
            directories_to_move.append(directory)
            print_green(f"Found directory to move: {directory}")
            logging.info(f"Found directory to move: {directory}")
    return directories_to_move

def move_directories(ssh, directories):
    # This method is used to sync file contents from remote to local pc. It then removes after files have been synced
    for directory in directories:
        command = f"sshpass -p {PASSWORD} rsync -avz {USERNAME}@{REMOTE_HOST}:{directory} {LOCAL_PATH}"
        os.system(command)
        print_green(f"Moved directory {directory} to local machine.")
        logging.info(f"Moved directory {directory} to local machine.")
        # Optionally, remove the directory after moving
        ssh.exec_command(f'rm -rf {directory}')
        print_green(f"Removed directory {directory} from remote machine.")
        logging.info(f"Removed directory {directory} from remote machine.")

def find_sbatch_files_from_directory(ssh):
    # Find all the batch files within the specified directory from home directory
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

def find_sbatch_files_from_json():
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

def run_sbatch(ssh):
    batch_files_from_directory = find_sbatch_files_from_directory(ssh)
    batch_files_from_json = list(find_sbatch_files_from_json())
    
    print(f'batch_files_from_directory:\t{batch_files_from_directory}')
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
        print(f'QUEUED LIST: {queued_jobs}')
        print(f'QUEUED JOBS: {queued_jobs[0][0]}')
        stdin, stdout, stderr = ssh.exec_command(f'cd {REMOTE_WORKING_PROJECT} ; cd {REMOTE_BATCH_FILE_LOCATION}/ ; ls -l')
        for counter, line in enumerate(stdout):
            print_green(f"Line 187: {line.strip()}")
        
        print(f'sbatch {REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
        # stdin, stdout, stderr = ssh.exec_command(f'cd {REMOTE_WORKING_PROJECT} ; sbatch {REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
        # for counter, line in enumerate(stdout):
        #     print_green(f"Line 189: {line.strip()}")
        running_item = queued_jobs[0][0]
        print(f"LINE 192: {running_item}")
        # PROBLEM OCCURING HERE 
        if running_item in queued_jobs:
            queued_jobs.remove(running_item)
        else:
            print_red("ERROR")
        update_json_wrapper(ssh)
    else:
        print_red("No jobs with status QUEUED")
    
def create_json(ssh):
    base_dir = '/'.join(os.path.join(REMOTE_WORKING_PROJECT, REMOTE_BATCH_FILE_LOCATION).replace("\\", "/").split('/')[:-1])
    print(f"Base Dir: {base_dir}")
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
    
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    # populate list of files that are found in the json file
    existing_filenames = {entry['filename'] for entry in dictionary_list}
    
    # Iterate over each status directory
    for sub_dir, status in status_directories.items():
        # Build the full path to the directory
        remote_dir = f"{base_dir}/{sub_dir}" 
    # Find the batch files stored in the remote batch file location
        print(remote_dir)
        # Execute command to list files in the directory
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
                
                # Extract the job names from the batch files to add into the json file
                stdin, stdout, stderr = ssh.exec_command(f'cat {remote_dir}/{filename}')
                job_name = ""
                working_directory = ""
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
                    'working_dirctory': working_directory,
                    'status': status
                }
                
                # Add new files to the json file to keep track of which files are run
                dictionary_list.append(file_dict)
                existing_filenames.add(filename)
                print_green(f"Added file {filename} to the JSON file.")
                logging.info(f"Added file {filename} to the JSON file.")
    
    with open(json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)

def update_json_wrapper(ssh):
    global last_status_counts
    last_status_counts = update_json(ssh)

def move_batch_files_based_on_status(ssh):
    # Load JSON data
    with open(json_file_path, 'r') as f:
        batch_files_data = json.load(f)

    for batch_file in batch_files_data:
        filename = batch_file['filename']
        status = batch_file['status']
        base_dir = os.path.join(REMOTE_WORKING_PROJECT, REMOTE_BATCH_FILE_LOCATION)
        base_dir = '/'.join(base_dir.split('/')[:-1])
        print(base_dir)
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

def update_json(ssh):
    # TODO REMOVE FILES FORM JSON FILE IF BATCH FILES ARE MOVED/NOT PRESENT
    # TODO IF ERROR, MOVE BATCH FILE TO ERROR DIRECTORY, CREATE AND UPDATE JSON
    dictionary_list = []
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    project_work_dir = f'{REMOTE_WORKING_PROJECT}/{REMOTE_WORK_DIR}'
    
    # Find all directories with the completed.txt file in it. 
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name completed.txt')
    output_complete = stdout.read().decode().strip().split('\n')
    output_complete_files = [(completed.replace("mmseg-personal/work_dirs/", "").replace("/completed.txt", ""), "COMPLETED") for completed in output_complete]
    # Find all directories with the error_occurred.txt file in it. 
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name error_occurred.txt')
    output_error = stdout.read().decode().strip().split('\n')
    output_error_files = [(error.replace("mmseg-personal/work_dirs/", "").replace("/error_occurred.txt", ""), "ERROR") for error in output_error]
    # Find all directories with the in_progress.txt file in it. 
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name in_progress.txt')
    output_progress = stdout.read().decode().strip().split('\n')
    output_progress_files = [(progress.replace("mmseg-personal/work_dirs/", "").replace("/in_progress.txt", ""), "RUNNING") for progress in output_progress]
    # Find all directories with the extracted.txt file in it. 
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name extracted.txt')
    output_extracted = stdout.read().decode().strip().split('\n')
    output_extracted_files = [(extracted.replace("mmseg-personal/work_dirs/", "").replace("/extracted.txt", ""), "FINISHED") for extracted in output_extracted]

    # Add/update status of files in json based off of which text file is found within the directories
    for completed in output_complete_files:
        completed_working_directory, completed_status = completed
        
        for entry in dictionary_list:
            if entry["working_dirctory"] == completed_working_directory:
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
            if entry["working_dirctory"] == error_working_directory:
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
            if entry["working_dirctory"] == progress_working_directory:
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
            if entry["working_dirctory"] == extracted_working_directory:
                if entry["status"] != extracted_status:
                    logging.info(f"Changing {entry['job_name']} status from {entry['status']} to {extracted_status}")
                    print(f"Changing {entry['job_name']} status from {entry['status']} to {extracted_status}")
                    entry["status"] = extracted_status
                    with open(json_file_path, 'w') as json_file:
                        json.dump(dictionary_list, json_file, indent=4)
                break 

    if len(output_complete_files) == 0 and len(output_error_files) == 0 and len(output_progress_files) == 0:
        print("No jobs are currently running")
        logging.info("No jobs are currently running")

    status_counter = {}
    for item in dictionary_list:
        status = item["status"]
        if status in status_counter:
            status_counter[status] += 1
        else:
            status_counter[status] = 1
    
    return status_counter.get('FINISHED', 0), status_counter.get('COMPLETED', 0), status_counter.get('ERROR', 0), status_counter.get('RUNNING', 0), status_counter.get('QUEUED', 0)

def log_extraction(ssh):
    
    try:
        # Find directories that have the completed.txt file indicating that training is done
        project_work_dir = f'{REMOTE_WORKING_PROJECT}/{REMOTE_WORK_DIR}'
        print(f'Project_Work_DIR:\t{project_work_dir}')
        stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {COMPLETED_MARKER_FILE}')
        output_complete = stdout.read().decode().strip().split('\n')
        print(output_complete)

        # If there are directories found that have completed training, execute this block
        #largest_json_files = []
        if output_complete != ['']:
            for completed_job in output_complete:
                # Remove the last entry in the path (i.e. DIRECTORY_MARKER_FILE)
                directory = '/'.join(completed_job.split('/')[:-1])
                # Find the largest json file, usually means that it has the most entries indicating it is the json file of the training session
                find_largest_json_file = f'find {directory} -type f -name "*.json" -exec ls -s {{}} + | sort -n | tail -n 1 | awk \'{{print $2}}\''
                stdin, stdout, stderr = ssh.exec_command(find_largest_json_file)
                largest_json = stdout.read().decode().strip()
                
                if largest_json:
                    #largest_json_files.append(largest_json)
                    largest_json_trunc = '/'.join(largest_json.split('/')[1:])
                    print(f'The largest JSON file located in this directory is: {largest_json_trunc}')
                    
                    log_extraction_python = f'cd {REMOTE_WORKING_PROJECT} ; python3 research/log_extraction.py --input {largest_json_trunc}'
                    stdin, stdout, stderr = ssh.exec_command(log_extraction_python)
                    # Output the result of the command
                    print(f"Executed command in directory {directory}:")
                    print(stdout.read().decode())
                    print(stderr.read().decode())
                    extracted_job = completed_job.replace("completed.txt", "extracted.txt")
                    
                    # Command to rename the file
                    rename_command = f'mv {completed_job} {extracted_job}'
                    # Execute the rename command on the remote server
                    stdin, stdout, stderr = ssh.exec_command(rename_command)
                    
                    # Check for errors
                    error = stderr.read().decode().strip()
                    if error:
                        print_red(f"Error renaming {completed_job}: {error}")
                    else:
                        print_green(f"Successfully renamed {completed_job} to {extracted_job}")

                else:
                    print_red(f'No JSON files were found in this directory: {directory}')
        else:
            print_red(f"{COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")   
    except Exception as e:
        print(f"An error occured: {str(e)}")

def run_every_hour(ssh):
    # Check how much storage is being used
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

# def nuke_all_sbatch(ssh):
#     stdin, stdout, stderr = ssh.exec_command(f'squeue --format="%.18i %.9P %.30j %.8u %.8T %.10M %.9l %.6D %R" --me')
#     all_jobs = []
#     for counter, line in enumerate(stdout):
#         if counter == 0:
#             continue
#         print(f'CANCEL ALL SBATCH: {line.split()[2]} {line.split()[4]}')
#         # 2 is job name
#         # 4 is status

def main():
    ssh = connect_ssh()
    create_json(ssh)
    # COMPLETED, ERROR, RUNNING, QUEUED = update_json(ssh)
    try:
        while True:
            run_every_hour(ssh)
            # directories_to_move = find_directories_to_move(ssh)
            # move_directories(ssh, directories_to_move)

            global last_status_counts
            update_json_wrapper(ssh)
            print(f'STATUS DICTIONARY: {last_status_counts}')
            
            if last_status_counts[3] < 4:
                # ERROR OCCURING HERE in queue_jobs.remove(running_item)
                run_sbatch(ssh)
            move_batch_files_based_on_status(ssh)
            
            log_extraction(ssh)
            time.sleep(60)
        # [DONE] TODO IF ERROR, MOVE BATCH FILE TO ERROR DIRECTORY, CREATE AND UPDATE JSON
        # TODO REMOVE FILES FORM JSON FILE IF BATCH FILES ARE MOVED/NOT PRESENT
        # TODO COME UP WITH LOGICAL FLOW OF OPERATIONS TO FIND, RUN, EXTRACT DATA AND RELOCATE FILES LOCALLY

        # run_every_hour(ssh)
        # schedule.every().hour.do(run_every_hour, ssh)

        # schedule.every().minute.do(run_every_hour, ssh)
        # schedule.every().minute.do(create_json, ssh)
        # schedule.every().hour.do(update_json, ssh)
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

        # TODO FIX MOVE_BATCH_FILES METHOD TO IGNORE FILES THAT DONT NEED TO BE MOVED
        # move_batch_files_based_on_status(ssh)
        # cancel_all_sbatch(ssh)
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)
        # print_green("SUCCESS")
    except Exception as e:
        print(e)
        logging.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()