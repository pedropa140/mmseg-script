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
DIRECTORY_MARKER_FILE = 'completed.txt'  # The file that indicates the directory should be moved
LOCAL_PATH = '/home/diez-lab/Corrosion_Detection/'



REMOTE_BASE_PATH = '/common/home/bn155'
REMOTE_WORKING_PROJECT = 'mmseg-personal'
REMOTE_WORK_DIR = 'work_dirs'
REMOTE_BATCH_FILE_LOCATION = 'tools/batch_files/not_started'
REMOTE_BATCH_FILE_PATH = 'mmseg-personal/tools/batch_files/not_started'

json_file_path = 'batch_files.json'
job_threshold = 3
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
    directories_to_move = []
    project_work_dir = os.path.join(REMOTE_BASE_PATH, REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR)
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {DIRECTORY_MARKER_FILE}')
    output = stdout.read().decode().strip().split('\n')
    for line in output:
        if line:  # Make sure it's not an empty line
            directory = os.path.dirname(line)
            directories_to_move.append(directory)
            print_green(f"Found directory to move: {directory}")
            logging.info(f"Found directory to move: {directory}")
    return directories_to_move

def move_directories(ssh, directories):
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
    return sbatch_files

def find_sbatch_files_from_json():
    # Find and return batch files shown in json_file 
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)

            # populate list of files that are found in the json file
        existing_filenames = {entry['filename'] for entry in dictionary_list}
        return existing_filenames
    
    return None

def run_sbatch(ssh):
    batch_files_from_directory = find_sbatch_files_from_directory(ssh)
    batch_files_from_json = list(find_sbatch_files_from_json())

    # print(f'batch_files_from_directory:\t{batch_files_from_directory}')
    # print(f"batch_files_from_json:\t{batch_files_from_json}")
    # Check json file
    # Find jobs to run by making sure we aren't rerunning already running jobs
        # compare squeue names with Json names and batch_file_directory names
        # find list of exclusive batchfiles, create a list, run the first one on list
        # move executed job into a new directory?

    pass    
    
def create_json(ssh):

    # Open json file to check which files are already accounted for. 
    dictionary_list = []
    json_file_path = 'batch_files.json'
    
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    # populate list of files that are found in the json file
    existing_filenames = {entry['filename'] for entry in dictionary_list}
    

    # Find the batch files stored in the remote batch file location
    print(REMOTE_BATCH_FILE_PATH)
    stdin, stdout, stderr = ssh.exec_command(f'cd  {REMOTE_BATCH_FILE_PATH}; ls -l')
    
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
            stdin, stdout, stderr = ssh.exec_command(f'cat {REMOTE_BATCH_FILE_PATH}/{filename}')
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
                'status': 'NOT STARTED'
            }
            
            # Add new files to the json file to keep track of which files are run/
            dictionary_list.append(file_dict)
            existing_filenames.add(filename)
            print_green(f"Added file {filename} to the JSON file.")
            logging.info(f"Added file {filename} to the JSON file.")
    
    with open(json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)

def update_json(ssh, processed_squeue_data):
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    # project_work_dir = os.path.join(REMOTE_BASE_PATH, REMOTE_WORKING_PROJECT, REMOTE_WORK_DIR)
    # project_work_dir = 'mmseg-personal/work_dirs/'

    project_work_dir = f'{REMOTE_WORKING_PROJECT}/{REMOTE_WORK_DIR}'

<<<<<<< HEAD
    # Look for files to indicate that job is COMPLETED
    print(project_work_dir)
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {DIRECTORY_MARKER_FILE}')
=======
    # Look for file to indicate that job is COMPLETED
    print(f'Project_Work_DIR:\t{project_work_dir}')
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name completed.txt')
>>>>>>> refs/remotes/origin/main
    output_complete = stdout.read().decode().strip().split('\n')

    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name error_occured.txt')
    output_error = stdout.read().decode().strip().split('\n')

    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name in_progress.txt')
    output_progress = stdout.read().decode().strip().split('\n')

    print(f'Output_Complete:\t{output_complete}')
    print(f'Output_Error:\t{output_error}')
    print(f'Output_Progress:\t{output_progress}')

    # Look for file to indicate that a job is IN PROGRESS
    if processed_squeue_data != None:
        for squeue_data in processed_squeue_data:
            job_name = squeue_data['NAME']
            state = squeue_data['STATE']

        
            # Find the corresponding entry in the JSON data
            for entry in dictionary_list:
                if entry["job_name"] == job_name:
                    # Update the status in the JSON entry
                    if entry["status"] != state:
                        logging.info(f"Changing {job_name} status from {entry['status']} to {state}")
                        print(f"Changing {job_name} status from {entry['status']} to {state}")
                        entry["status"] = state
                    break  # Exit the loop since we found the matching entry
        
        with open(json_file_path, 'w') as json_file:
            json.dump(dictionary_list, json_file, indent=4)
    else:
        print("No jobs are currently running")
        logging.info("No jobs are currently running")

def check_squeue(ssh):

    # # TEST CODE FOR RUNNING AND CANCELING ONE BATCH FILE
    # filtered_list = []
    # stdin, stdout, stderr = ssh.exec_command(f'cd {REMOTE_WORKING_PROJECT} ; sbatch {REMOTE_BATCH_FILE_LOCATION}/hrnet18-fcn-automation_test.batch')
    # for counter, line in enumerate(stdout):
    #     print(f'LINE 268:\t{line}')
    
    # time.sleep(10)

    # dictionary_list = []
    # json_file_path = 'batch_files.json'
    
    # if os.path.exists(json_file_path):
    #     with open(json_file_path, 'r') as json_file:
    #         dictionary_list = json.load(json_file)
    
    # print(dictionary_list)
    
    # Check with squeue to see which jobs are running by the user
    processed_data = []
    running_count = 0
    pending_count = 0
    
    stdin, stdout, stderr = ssh.exec_command(f'squeue --format="%.18i %.9P %.30j %.8u %.8T %.10M %.9l %.6D %R" --me')
    output = stdout.read().decode()
    squeue_jobs = output.splitlines()
    if len(squeue_jobs) > 1:
        # print(f"SQUEUE JOBS: \t{squeue_jobs}")
        # Process output from squeue and display JOBID, NAME, STATE, and TIME
        header = squeue_jobs[0].split()
        for row in squeue_jobs[1:]:
            values = row.split()
            entry = dict(zip(header, values))
            processed_data.append(entry)
        
        
        keys = processed_data[0].keys()
        keys_list = list(keys)
        for item in processed_data:
            job_info_string = f"{keys_list[0]}: {item[keys_list[0]]}, {keys_list[2]}: {item[keys_list[2]]}, {keys_list[4]}: {item[keys_list[4]]}, {keys_list[5]}: {item[keys_list[5]]}"
            print(job_info_string)
            logging.info(job_info_string)
        for item in processed_data:
            state = item['STATE']
            if state == 'RUNNING':
                running_count += 1
            elif state == 'PENDING':
                pending_count += 1

        # Print the results
        print(f"Number of RUNNING files: {running_count}")
        print(f"Number of PENDING files: {pending_count}")
    else:
        return None, 0, 0
    # for counter, line in enumerate(stdout):
    #     if counter == 0:
    #         continue
    #     lines = line.strip().split(" ")
    #     filtered_list = [s for s in lines if s][2]

    # print(len(filtered_list))

    
    # # TEST CODE FOR RUNNING AND CANCELING ONE BATCH FILE 
    # time.sleep(5)
    # stdin, stdout, stderr = ssh.exec_command(f'scancel -n {filtered_list[2]}')
    # for counter, line in enumerate(stdout):
    #     if counter == 0:
    #         continue
    #     print(line)
    return processed_data, running_count, pending_count

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

    # Check how many jobs are running
    job_info, num_of_running_jobs, num_of_pending_jobs = check_squeue(ssh)
    
    if (num_of_running_jobs + num_of_pending_jobs) < job_threshold:
        # Run sbatch on next available file
        # TODO IMPLEMENT FINDING EXCLUSIVE BATCH FILES TO RUN 
        pass

# def cancel_all_sbatch(ssh):
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
    try:
        # run_every_hour(ssh)
        # schedule.every().hour.do(run_every_hour, ssh)

        # schedule.every().minute.do(run_every_hour, ssh)c
        # schedule.every().minute.do(create_json, ssh)

        # run 3 at a time
        # create json of file names saying if completed or not

        processed_squeue_data, running_jobs, pending_jobs = check_squeue(ssh)
        update_json(ssh, processed_squeue_data)
        if running_jobs < 4:
            run_sbatch(ssh)

        # cancel_all_sbatch(ssh)
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)
        print_green("SUCCESS")
    except Exception as e:
        print(e)
        logging.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()