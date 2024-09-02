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
# import utils
import json_utils
import config as cfg
import remote_operations as rops
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
# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

last_status_counts = None
queued_jobs = []


def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_red(text):
    print(f"\033[91m{text}\033[0m")

def print_blue(text):
    print(f"\033[38;2;50;128;128m{text}\033[0m")

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
    project_work_dir = os.path.join(cfg.REMOTE_BASE_PATH, cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR).replace("\\", "/")
    logging.info(f"Executing: find {project_work_dir} -name {cfg.FINISHED_MARKER_FILE}")
    
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {cfg.FINISHED_MARKER_FILE}')
    
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
            if cfg.linux:
                logging.info(f"Executing: rsync -avz '{cfg.USERNAME}@{cfg.REMOTE_HOST}:{directory}', {cfg.LOCAL_PATH}")
                print_red(f"Executing: rsync -avz '{cfg.USERNAME}@{cfg.REMOTE_HOST}:{directory}', {cfg.LOCAL_PATH}")
                # UNCOMMENT SSHPASS LINE IF RUNNING ON LAB PC                
                command = [
                #    'sshpass', '-p', PASSWORD,
                    'rsync', '-avz',
                    f'{cfg.USERNAME}@{cfg.REMOTE_HOST}:{directory}', cfg.LOCAL_PATH
                ]
                result_rsync = subprocess.run(command, capture_output=True, text=True)

                # Check if rsync was successful                
                if result_rsync.returncode != 0:
                    logging.error(f"rsync failed with error: {result_rsync.stderr}")
                    raise Exception(f"rsync failed with error: {result_rsync.stderr}")
                
            if cfg.windows:
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
                pscp_cmd = [cfg.PSCP_PATH, "-r", "-pw", cfg.PASSWORD, f"{cfg.USERNAME}@{cfg.REMOTE_HOST}:{directory}", cfg.LOCAL_PATH]
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

            print_green(f"Moved directory {directory} to local machine: {cfg.LOCAL_PATH}")
            logging.info(f"Moved directory {directory} to local machine: {cfg.LOCAL_PATH}")
            
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
def check_batch_files(ssh, jobs):
    base_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION).replace("\\", "/")
    base_dir = '/'.join(base_dir.split('/')[:-1])  # Remove the last part (_QUEUED) to get the main directory
    
    dirs_to_check = [d for d in rops.list_remote_directories(ssh, base_dir) if d.startswith('_')]
    logging.info(f"Checking batch_files in {base_dir} in these directories: {dirs_to_check}")
    for dir_name in dirs_to_check:
        full_dir_path = os.path.join(base_dir, dir_name).replace("\\", "/")
        for filename in rops.list_remote_files(ssh, full_dir_path):
            batch_file_path = os.path.join(full_dir_path, filename).replace("\\", "/")
            job_name = rops.get_job_name_from_batch_file(ssh, batch_file_path)
            print(job_name)
            matching_job = next((job for job in jobs if job["name"] == job_name), None)
            # Determine if any running job from the batch file directory that isn't already in _RUNNING is moved to _RUNNING 
            if matching_job:
                if dir_name != "_RUNNING":
                    rops.move_batch_file(ssh, batch_file_path, os.path.join(base_dir, "_RUNNING").replace("\\", "/"))
            else:
                check_and_handle_non_running_job(ssh, job_name, batch_file_path, base_dir)

    # Additional step: Handle jobs that are no longer in squeue
    handle_cancelled_jobs(ssh, jobs, base_dir)
# COMPLETED
def check_and_handle_non_running_job(ssh, job_name, batch_file_path, base_dir):
    logging.info(f"Check and handle non running jobs: job-name : {job_name}, batch_file_path : {batch_file_path} in directory {base_dir})")
    python_file_name = rops.get_python_file_name_from_batch_file(ssh, batch_file_path)
    work_dir_path = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, python_file_name).replace("\\", "/")
    in_progress_file = os.path.join(work_dir_path, "in_progress.txt").replace("\\", "/")
    error_file = os.path.join(work_dir_path, "error_occurred.txt").replace("\\", "/")

    # If there is an in_progress.txt file, but there is no running job, then move the batch file over. 
    if rops.check_remote_file_exists(ssh, in_progress_file):
        rops.rename_remote_file(ssh, in_progress_file, error_file)
        # If the batch file is not ALREADY in error directory, move it over to error directory. 
        if batch_file_path.split('/')[3] != '_ERROR':
            print(f"Batch File {os.path.basename(batch_file_path)} is not in _ERROR directory. Moving to _ERROR/")
            logging.info(f"Batch File {os.path.basename(batch_file_path)} is not in _ERROR directory. Moving to _ERROR/")
            rops.move_batch_file(ssh, batch_file_path, os.path.join(base_dir, "_ERROR").replace("\\", "/"))
# COMPLETED
def handle_cancelled_jobs(ssh, jobs, base_dir):
    logging.info(f"Handle cancelled jobs: Jobs: {jobs}, in directory: {base_dir}) ---")
    # Check all work_dirs for in_progress.txt files and handle those that are no longer running
    work_dirs = rops.list_remote_directories(ssh, os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR).replace("\\", "/"))
    for work_dir in work_dirs:
        work_dir_path = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, work_dir).replace("\\", "/")
        in_progress_file = os.path.join(work_dir_path, "in_progress.txt").replace("\\", "/")
        error_file = os.path.join(work_dir_path, "error_occurred.txt").replace("\\", "/")

        if rops.check_remote_file_exists(ssh, in_progress_file):
            # Extract the job name from the batch file associated with this work_dir
            batch_file_name = rops.find_associated_batch_file(ssh, base_dir, work_dir)
            logging.info(f"Handling cancelled job: {batch_file_name}")
            print(f"Handling cancelled job: {batch_file_name}")
            if batch_file_name:
                job_name = rops.get_job_name_from_batch_file(ssh, batch_file_name)
                matching_job = next((job for job in jobs if job["name"] == job_name), None)
                if not matching_job:
                    # Job is no longer running, so mark as error and move batch file to _ERROR
                    rops.rename_remote_file(ssh, in_progress_file, error_file)
                    rops.move_batch_file(ssh, batch_file_name, os.path.join(base_dir, "_ERROR").replace("\\", "/"))
# COMPLETED
def run_sbatch(ssh):
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
        logging.info(f"Executing command: cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}")
        stdin, stdout, stderr = ssh.exec_command(f'cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
        for counter, line in enumerate(stderr):
            print_red(f"Line 193: {line.strip()}")
        if stderr == '':
            print_red("You may have to manually run kinit again. Please do this ASAP!")
            logging.error("You may have to manually run 'kinit' again. Please do this ASAP!")
        for counter, line in enumerate(stdout):
            print_green(f"Line 193: {line.strip()}")
            logging.info(f"SBATCH successful: {line.strip()}")
        time.sleep(1)
        jobs = rops.get_squeue_jobs(ssh)
        matching_job = next((job for job in jobs if job['name']==queued_jobs[0][1]), None)
        if matching_job:
            json_utils.set_status_of_batch_file("RUNNING", batch_file=queued_jobs[0][0])
            logging.info(f"{queued_jobs[0][0]} is running!")
            queued_jobs.remove((running_item, queued_jobs[0][1]))
        else: 
            logging.info(f"Rerunning command: cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}")
            stdin, stdout, stderr = ssh.exec_command(f'cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
            for counter, line in enumerate(stderr):
                print_red(f"Line 193: {line.strip()}")
            if stderr == '':
                print_red("You may have to manually run kinit again. Please do this ASAP!")
                logging.error("You may have to manually run 'kinit' again. Please do this ASAP!")
            for counter, line in enumerate(stdout):
                print_green(f"Line 193: {line.strip()}")
            time.sleep(5)
            jobs = rops.get_squeue_jobs(ssh)
            matching_job = next((job for job in jobs if job['name']==queued_jobs[0][1]), None)
            if matching_job:
                json_utils.set_status_of_batch_file("RUNNING", batch_file=queued_jobs[0][0])
                queued_jobs.remove((running_item, queued_jobs[0][1]))
                logging.info(f"{queued_jobs[0][0]} is running!")
            else:
                json_utils.set_status_of_batch_file("ERROR", batch_file=queued_jobs[0][0])
                queued_jobs.remove((running_item, queued_jobs[0][1]))
                logging.error(f"{queued_jobs[0][0]} is not running. Double check issue with model.")
        json_utils.update_json_new(ssh)
    else:
        print("No jobs with status QUEUED")
        logging.info("No jobs with status QUEUED")
# COMPLETED
def move_batch_files_based_on_status(ssh):
    logging.info("Move batch files to their folders based off status in json file")
    # Load JSON data
    with open(cfg.json_file_path, 'r') as f:
        batch_files_data = json.load(f)

    for batch_file in batch_files_data:
        filename = batch_file['filename']
        status = batch_file['status']
        base_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION).replace("\\", "/")
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
        project_work_dir = f'{cfg.REMOTE_WORKING_PROJECT}/{cfg.REMOTE_WORK_DIR}'
        # print(f'Project_Work_dir:\t{project_work_dir}')
        logging.info(f'find {project_work_dir} -name {cfg.COMPLETED_MARKER_FILE}')
        stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {cfg.COMPLETED_MARKER_FILE}')
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
                    logging.info(f'cd {cfg.REMOTE_WORKING_PROJECT} ; python3 research/log_extraction.py --input {largest_json_trunc}')
                    log_extraction_python = f'cd {cfg.REMOTE_WORKING_PROJECT} ; python3 research/log_extraction.py --input {largest_json_trunc}'
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
            logging.error(f"{cfg.COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
            print_red(f"{cfg.COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        print(f"An error occured: {str(e)}")

def run_every_hour(ssh):
    print("Testing scheduling!")
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
    if usage_percentage > cfg.THRESHOLD:
        directories = find_directories_to_move(ssh)
        if directories:
            #move_directories(ssh, directories)
            print(f"Would move these directories {directories}")
        else:
            print_red("No directories found to move")
            logging.info("No directories found to move.")
    else:
        print_green("Storage usage is within limits.")
        logging.info("Storage usage is within limits.")
    last_status_counts = json_utils.update_json_new(ssh)
    print(f'STATUS DICTIONARY:\nFinished = {last_status_counts[0]} \nCompleted = {last_status_counts[1]} '\
          f'\nError = {last_status_counts[2]} \nRunning = {last_status_counts[3]} \nQueued = {last_status_counts[4]}')
    jobs=rops.get_squeue_jobs(ssh)
    check_batch_files(ssh, jobs)
    if last_status_counts[3] < cfg.THRESHOLD:
        run_sbatch(ssh)
        move_batch_files_based_on_status(ssh)
    # Look and extract logs for jobs that are completed. 
    log_extraction(ssh)
    last_status_counts = json_utils.update_json_new(ssh)
    print(f'STATUS DICTIONARY:\nFinished = {last_status_counts[0]} \nCompleted = {last_status_counts[1]} '\
          f'\nError = {last_status_counts[2]} \nRunning = {last_status_counts[3]} \nQueued = {last_status_counts[4]}')


def test_model(ssh):
    # TODO Implement python tools/test.py custom_config.py work_dirs/[job-name]/best_*.pth --eval mIoU --show-dir work_dir/[job-name]/best_*_iter_output function call on completed models
    return NotImplementedError

def main():
    # TODO FIX STATUS UPDATES FOR RUNNING MODELS... We might not be clearing lists to queue and sbatch models properly
    run_counter = 0
    ssh = rops.connect_ssh(remote_host=cfg.REMOTE_HOST, username=cfg.USERNAME, password=cfg.PASSWORD)
    json_utils.create_json(ssh)
    schedule.every().minute.do(run_every_hour, ssh)
    try:
        while True:
            # Run all pending scheduled tasks
            schedule.run_pending()
            time.sleep(21)
            print("21 Seconds passed...")
        # if last_status_counts[3] < cfg.THRESHOLD:
        #         run_sbatch(ssh)
        #         move_batch_files_based_on_status(ssh)

        # jobs = rops.get_squeue_jobs(ssh)
        # print(jobs)
        # check_batch_files(ssh, jobs)

        #move_batch_files_based_on_status(ssh)

        # Check storage usage and move directories to local pc if they are finished and storage is above a certain memory threshold
        #run_every_hour(ssh)

        # Look and extract logs for jobs that are completed. 
        #log_extraction(ssh)
        # last_status_counts = json_utils.update_json_new(ssh)
        # print(f'STATUS DICTIONARY:\nFinished = {last_status_counts[0]} \nCompleted = {last_status_counts[1]} \nError = {last_status_counts[2]} \nRunning = {last_status_counts[3]} \nQueued = {last_status_counts[4]}')
        # run_counter+=1
        # print_green(f"Program Complete counter: {run_counter}")


    except Exception as e:
        print(e)
        logging.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()