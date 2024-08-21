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
REMOTE_BASE_PATH = '/common/home/bn155/mmseg-personal/work_dirs/'
REMOTE_BATCH_FILE_PATH = 'mmseg-personal/tools/batch_files/not_started'

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
    stdin, stdout, stderr = ssh.exec_command(f'find {REMOTE_BASE_PATH} -name {DIRECTORY_MARKER_FILE}')
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

def run_every_hour(ssh):
    usage_percentage = check_storage_usage(ssh)
    if usage_percentage:
        print_green(f"Storage usage is at {usage_percentage:.2f}%.")
        logging.info(f"Storage usage is at {usage_percentage:.2f}%.")
    else:
        print_red("Could not determine storage usage.")
        logging.info("Could not determine storage usage.")
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

def create_json(ssh):
    dictionary_list = []
    json_file_path = 'batch_files.json'
    
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    existing_filenames = {entry['filename'] for entry in dictionary_list}
    
    stdin, stdout, stderr = ssh.exec_command('cd mmseg-personal/tools/batch_files/not_started/ ; ls -l')
    
    for counter, line in enumerate(stdout):
        if counter == 0:
            continue
        
        # Split the line by whitespace
        parts = line.strip().split()
        
        # Check if the entry is a file (not a directory)
        if len(parts) >= 9 and parts[0].startswith('-'):  # Files have '-' at the start of the permission string
            filename = parts[8]
            
            if filename in existing_filenames:
                print_red(f"File {filename} is already in the JSON file.")
                continue
            
            stdin, stdout, stderr = ssh.exec_command(f'cat mmseg-personal/tools/batch_files/not_started/{filename}')
            job_name = ""
            for line in stdout:
                line = line.strip()
                if "#SBATCH --job-name=" in line:
                    job_name = line.replace('#SBATCH --job-name=', '').replace(' ', '')
                    break
            
            file_dict = {
                'filename': filename,
                'job_name': job_name,
                'status': 'not started'
            }
            
            dictionary_list.append(file_dict)
            existing_filenames.add(filename)
            print_green(f"Added file {filename} to the JSON file.")
    
    with open(json_file_path, 'w') as json_file:
        json.dump(dictionary_list, json_file, indent=4)

def send_sbatch(ssh):
    stdin, stdout, stderr = ssh.exec_command(f'find {REMOTE_BATCH_FILE_PATH} -name "*.batch"')
    output = stdout.read().decode()
    print(output)
    pass
    #return NotImplementedError

def check_squeue(ssh):
    filtered_list = []
    stdin, stdout, stderr = ssh.exec_command('cd mmseg-personal ; sbatch tools/batch_files/not_started/hrnet18-fcn-automation_test.batch')
    for counter, line in enumerate(stdout):
        print(line)
    
    time.sleep(10)

    dictionary_list = []
    json_file_path = 'batch_files.json'
    
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)
    
    print(dictionary_list)
            
    stdin, stdout, stderr = ssh.exec_command(f'squeue --format="%.18i %.9P %.30j %.8u %.8T %.10M %.9l %.6D %R" --me')
    for counter, line in enumerate(stdout):
        if counter == 0:
            continue
        lines = line.strip().split(" ")
        filtered_list = [s for s in lines if s][2]

    print(filtered_list)

    
    time.sleep(5)
    stdin, stdout, stderr = ssh.exec_command(f'scancel -n {filtered_list[2]}')
    for counter, line in enumerate(stdout):
        if counter == 0:
            continue
        print(line)
    
def main():
    ssh = connect_ssh()
    create_json(ssh)
    try:
        # run_every_hour(ssh)
        # schedule.every().hour.do(run_every_hour, ssh)

        # schedule.every().minute.do(run_every_hour, ssh)
        # schedule.every().minute.do(create_json, ssh)

        # run 3 at a time
        # create json of file names saying if completed or not
        send_sbatch(ssh)
        check_squeue(ssh)
        
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()
