import paramiko
import logging
import time
import os
from getpass import getpass
import re
import subprocess
from dotenv import load_dotenv
import schedule

load_dotenv()

# Configuration
REMOTE_HOST = 'ilab4.cs.rutgers.edu'
USERNAME = 'bn155'
THRESHOLD = 45  # Set your threshold percentage
DIRECTORY_MARKER_FILE = 'completed.txt'  # The file that indicates the directory should be moved
LOCAL_PATH = '/home/diez-lab/Corrosion_Detection/'
REMOTE_BASE_PATH = '/common/home/bn155/mmseg-personal/work_dirs/'

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Global SSH connection
ssh = None

def connect_ssh():
    global ssh
    if ssh is None:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # PASSWORD = getpass("Enter your SSH password: ")
        PASSWORD = os.getenv('password')
        ssh.connect(hostname=REMOTE_HOST, username=USERNAME, password=PASSWORD)

def check_storage_usage():
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


def find_directories_to_move():
    directories_to_move = []
    stdin, stdout, stderr = ssh.exec_command(f'find {REMOTE_BASE_PATH} -name {DIRECTORY_MARKER_FILE}')
    output = stdout.read().decode().strip().split('\n')
    for line in output:
        if line:  # Make sure it's not an empty line
            directory = os.path.dirname(line)
            directories_to_move.append(directory)
            print(f"Found directory to move: {directory}")
            logging.info(f"Found directory to move: {directory}")
    return directories_to_move

def move_directories(directories):
    for directory in directories:
        command = f"rsync -avz {USERNAME}@{REMOTE_HOST}:{directory} {LOCAL_PATH}"
        os.system(command)
        print(f"Moved directory {directory} to local machine.")
        logging.info(f"Moved directory {directory} to local machine.")
        # Optionally, remove the directory after moving
        ssh.exec_command(f'rm -rf {directory}')
        print(f"Removed directory {directory} from remote machine.")
        logging.info(f"Removed directory {directory} from remote machine.")

def monitor_storage():
    usage_percentage = check_storage_usage()
    if usage_percentage:
        print(f"Storage usage is at {usage_percentage:.2f}%.")
        logging.info(f"Storage usage is at {usage_percentage:.2f}%.")
    else:
        print("Could not determine storage usage.")
        logging.info("Could not determine storage usage.")

    if usage_percentage and usage_percentage > THRESHOLD:
        directories = find_directories_to_move()
        if directories:
            move_directories(directories)
        else:
            print("No directories found to move.")
            logging.info("No directories found to move.")
    else:
        logging.info("Storage usage is within limits.")

def run_monitoring():
    monitor_storage()

def main():
    global ssh
    connect_ssh()
    try:
        schedule.every().hour.do(run_monitoring)
        while True:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        if ssh is not None:
            ssh.close()
            ssh = None    

if __name__ == '__main__':
    main()
