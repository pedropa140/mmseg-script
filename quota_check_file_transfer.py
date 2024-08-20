import paramiko
import logging
import time
import os
from getpass import getpass
import re
import subprocess


# Configuration
REMOTE_HOST = 'ilab4.cs.rutgers.edu'
USERNAME = 'bn155'
THRESHOLD = 80  # Set your threshold percentage
DIRECTORY_MARKER_FILE = 'move_me.txt'  # The file that indicates the directory should be moved
LOCAL_PATH = '/path/to/local/destination'
REMOTE_BASE_PATH = '/remote/path/to/check'

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_ssh():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    PASSWORD = getpass("Enter your SSH password: ")
    ssh.connect(hostname=REMOTE_HOST, username=USERNAME, password=PASSWORD)
    return ssh

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

                print(f"Usage Percentage: {usage_percentage:.2f}%")
                return usage_percentage

    return None  # If the line wasn't found


def find_directories_to_move(ssh):
    directories_to_move = []
    stdin, stdout, stderr = ssh.exec_command(f'find {REMOTE_BASE_PATH} -name {DIRECTORY_MARKER_FILE}')
    output = stdout.read().decode().strip().split('\n')
    for line in output:
        if line:  # Make sure it's not an empty line
            directory = os.path.dirname(line)
            directories_to_move.append(directory)
            logging.info(f"Found directory to move: {directory}")
    return directories_to_move

def move_directories(ssh, directories):
    for directory in directories:
        command = f"rsync -avz {USERNAME}@{REMOTE_HOST}:{directory} {LOCAL_PATH}"
        os.system(command)
        logging.info(f"Moved directory {directory} to local machine.")
        # Optionally, remove the directory after moving
        ssh.exec_command(f'rm -rf {directory}')
        logging.info(f"Removed directory {directory} from remote machine.")

def main():
    ssh = connect_ssh()
    usage = check_storage_usage(ssh)
    try:
        usage_percentage = check_storage_usage(ssh)
        if usage_percentage:
            print(f"Storage usage is at {usage_percentage:.2f}%.")
        else:
            print("Could not determine storage usage.")
    finally:
        ssh.close()
    '''
    try:
        while True:
            usage = check_storage_usage(ssh)
            if usage > THRESHOLD:
                directories = find_directories_to_move(ssh)
                if directories:
                    move_directories(ssh, directories)
                else:
                    logging.info("No directories found to move.")
            else:
                logging.info("Storage usage is within limits.")
            time.sleep(3600)  # Sleep for an hour
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        ssh.close()
    '''

if __name__ == '__main__':
    main()
