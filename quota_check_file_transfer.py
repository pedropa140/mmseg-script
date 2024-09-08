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
    i.e. local_path=/home/diez-lab/Corrosion_Detection/model_outputs
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

global queued_jobs
global seen_batch_files
queued_jobs = []
seen_batch_files = set()
dictionary_list = []

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
    # FINDING FILES OF EXTRACTED.TXT WITHHOUT CHECKING THE STATUS. MIGHT CAUSE CONFLICTS
    logging.info(f"Executing: find {project_work_dir} -name {cfg.FINISHED_MARKER_FILE}")
    
    stdin, stdout, stderr = ssh.exec_command(f'find {project_work_dir} -name {cfg.FINISHED_MARKER_FILE}')
    
    stderr_output = stderr.read().decode().strip()
    if stderr_output:
        logging.error(stderr_output.split('\n'))
        print_red(stderr_output.split('\n'))
    
    output = stdout.read().decode().strip().split('\n')
    logging.info(f"Directories to move: {output}")
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
                    'sshpass', '-p', cfg.PASSWORD,
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
            stdin, stdout, stderr = ssh.exec_command(f'rm -rf {directory}')
            error = stderr.read().decode().strip()
            if error:
                raise Exception(f"Failed to remove directory {directory} on remote machine: {error}")

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
            # print(job_name)
            matching_job = next((job for job in jobs if job["name"] == job_name), None)
            # Determine if any running job from the batch file directory that isn't already in _RUNNING is moved to _RUNNING 
            if matching_job:
                if dir_name != "_RUNNING":
                    rops.move_batch_file(ssh, batch_file_path, os.path.join(base_dir, "_RUNNING").replace("\\", "/"))
                    json_utils.set_status_of_batch_file("RUNNING", os.path.basename(batch_file_path))
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
            json_utils.set_status_of_batch_file("ERROR", os.path.basename(batch_file_path))
# COMPLETED
def handle_cancelled_jobs(ssh, jobs, base_dir):
    logging.info(f"Handle cancelled jobs: Jobs: {jobs}, in directory: {base_dir}) ---")
    # Check all work_dirs for in_progress.txt files and handle those that are no longer running
    work_dirs = rops.list_remote_directories(ssh, os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR).replace("\\", "/"))
    for work_dir in work_dirs:
        work_dir_path = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, work_dir).replace("\\", "/")
        in_progress_file = os.path.join(work_dir_path, "in_progress.txt").replace("\\", "/")
        error_file = os.path.join(work_dir_path, "error_occurred.txt").replace("\\", "/")
# DOUBLE CHECK THIS METHOD and COMPARE WITH LINE 341 FOR STORAGE_MONITOR.LOG
        if rops.check_remote_file_exists(ssh, in_progress_file) == 'exists':
            # Extract the job name from the batch file associated with this work_dir
            batch_file_name = rops.find_associated_batch_file(ssh, base_dir, work_dir)
            logging.info(f"Handling cancelled job: {batch_file_name}")
            print(f"Handling cancelled job: {batch_file_name}")
            if batch_file_name:
                job_name = rops.get_job_name_from_batch_file(ssh, batch_file_name)
                matching_job = next((job for job in jobs if job["name"] == job_name), None)
                if matching_job == None:
                    # Job is no longer running, so mark as error and move batch file to _ERROR
                    rops.rename_remote_file(ssh, in_progress_file, error_file)
                    rops.move_batch_file(ssh, batch_file_name, os.path.join(base_dir, "_ERROR").replace("\\", "/"))
                    json_utils.set_status_of_batch_file("ERROR", os.path.basename(batch_file_name))

def remove_job(filename):
    # Use list comprehension to remove the job from queued_jobs if it matches the filename
    global queued_jobs
    global seen_batch_files
    queued_jobs = [job for job in queued_jobs if job[0][0] != filename]
    
    # Remove from the set if it exists
    logging.info(f"Removing ({queued_jobs[0][0]}, {queued_jobs[0][1]} from queued_jobs list. ")
    seen_batch_files.discard(filename)  # Use remove(filename) if you want an error to be raised if not found
    queued_jobs.remove((queued_jobs[0][0], queued_jobs[0][1]))

# COMPLETED
def run_sbatch(ssh):
    global queued_jobs
    global seen_batch_files
    dictionary_list = []
    json_file_path = 'batch_files.json'
    print()
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            dictionary_list = json.load(json_file)

    
    for item in dictionary_list:
        if item['status'] == 'QUEUED' :
            filename = item['filename']
            job_name = item['job_name']
            
             # Check if the filename is already in the set
            if filename not in seen_batch_files:
                # If not, add it to the queued_jobs list and mark it as seen
                job_tuple = (filename, job_name)
                queued_jobs.append(job_tuple)
                seen_batch_files.add(filename)
    running_item = ""
    if len(queued_jobs) > 0:
        gpu_initialized = rops.ssh_kinit_loop(1)
        if gpu_initialized:
            running_item = queued_jobs[0][0]
            print(f'QUEUED LIST: {queued_jobs}')
            logging.info(f'QUEUED LIST: {queued_jobs}')
            print(f'SELECTED JOB: {running_item}')
            logging.info(f'SELECTED JOB: {running_item}')
            
            logging.info(f"Executing command: cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}")
            stdin, stdout, stderr = ssh.exec_command(f'cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
            # Location of batch file within the QUEUED directory
            source_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION, queued_jobs[0][0]).replace('\\','/')
            dest_dir_running = os.path.join(cfg.REMOTE_WORKING_PROJECT, *cfg.REMOTE_BATCH_FILE_LOCATION.split('/')[:-1],"_RUNNING").replace('\\', '/')
            # if stderr.read().decode() == '':
            #     print_red(f"{stderr.read().decode()}")
            #     print_red("You may have to manually run kinit again. Please do this ASAP!")
            #     logging.error("You may have to manually run 'kinit' again. Please do this ASAP!")
            for counter, line in enumerate(stdout):
                print_green(f"{line.strip()}")
                logging.info(f"SBATCH successful: {line.strip()}")
            time.sleep(5)
            jobs = rops.get_squeue_jobs(ssh)
            matching_job = next((job for job in jobs if job['name']==queued_jobs[0][1]), None)

            #print(f"from run_sbatch() - Matching Job: {matching_job}")
            if matching_job != None:
                if matching_job['state'] == "RUNNING":
                    json_utils.set_status_of_batch_file("RUNNING", batch_file=queued_jobs[0][0])
                    rops.move_batch_file(ssh, source_dir, dest_dir_running)
                    logging.info(f"{queued_jobs[0][0]} is running!")
                    remove_job(queued_jobs[0][0]) 
            else:
                logging.info(f"Rerunning command: cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}")
                stdin, stdout, stderr = ssh.exec_command(f'cd {cfg.REMOTE_WORKING_PROJECT} ; sbatch {cfg.REMOTE_BATCH_FILE_LOCATION}/{queued_jobs[0][0]}')
                # for counter, line in enumerate(stderr):
                #     print_red(f"{line.strip()}")
                # if stderr == '':
                #     print_red("You may have to manually run kinit again. Please do this ASAP!")
                #     logging.error("You may have to manually run 'kinit' again. Please do this ASAP!")
                for counter, line in enumerate(stdout):
                    print_green(f"{line.strip()}")
                time.sleep(5)
                jobs = rops.get_squeue_jobs(ssh)
                matching_job = next((job for job in jobs if job['name']==queued_jobs[0][1]), None)
                if matching_job != None:
                    if matching_job['status'] == "RUNNING":
                        json_utils.set_status_of_batch_file("RUNNING", batch_file=queued_jobs[0][0])
                        rops.move_batch_file(ssh, source_dir, dest_dir_running)
                        logging.info(f"{queued_jobs[0][0]} is running!")
                        remove_job(queued_jobs[0][0])
                else:
                    json_utils.set_status_of_batch_file("ERROR", batch_file=queued_jobs[0][0])
                    working_directory = rops.get_python_file_name_from_batch_file(ssh, os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION, queued_jobs[0][0]).replace('\\','/'))
                    # ssh.exec_command(f"touch {os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, working_directory, "error_occurred.txt").replace("\\", "/")}")
                    
                    project_directory = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, working_directory).replace('\\','/')
                    stdin, stdout, stderr = ssh.exec_command(f"find {project_directory} -name *.txt")
                    
                    if stdout:
                        # Rename a textfile if there is one already in the working directory 
                        source_directory = stdout.read().decode().strip()
                        dest_directory = os.path.join(project_directory, "error_occurred.txt").replace('\\','/')
                        logging.info(f"Executing command: mv {source_directory} {dest_directory}")
                        rops.rename_remote_file(ssh, source_directory, dest_directory)
                    else:
                        # Add a new error_occurred text file in the working directory if there isn't a textfile found. 
                        error_textfile_path = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, working_directory, 'error_occurred.txt').replace("\\", '/')
                        logging.info(f"Executing command: touch {error_textfile_path}")
                        stdin, stdout, stderr = ssh.exec_command(f"touch {error_textfile_path}")
                        if stderr:
                            logging.error(stderr.read().decode())
                    # Add logic to move batch file VVV
                    batch_file_source = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION, queued_jobs[0][0]).replace('\\', '/')
                    batch_file_dest = os.path.join(cfg.REMOTE_WORKING_PROJECT, *cfg.REMOTE_BATCH_FILE_LOCATION.split('/')[:-1],"_ERROR").replace('\\', '/')
                    print_green(f"Moving {batch_file_source} to {batch_file_dest}") 
                    rops.move_batch_file(ssh, batch_file_source, batch_file_dest)
                    if rops.check_remote_file_exists(ssh, os.path.join(batch_file_dest, queued_jobs[0][0]).replace('\\','/')):
                        print_green(f"File {queued_jobs[0][0]} successfully moved to {batch_file_dest}.")
                    else:
                        print_red(f"File {queued_jobs[0][0]} was not moved successfully to {batch_file_dest}.")
                    #queued_jobs.remove((running_item, queued_jobs[0][1]))
                    logging.error(f"{queued_jobs[0][0]} is not running. Double check issue with model.")
                    remove_job(queued_jobs[0][0])
            json_utils.update_json_new(ssh)
        else:
            print_red("May need to run kinit again to start running jobs")
            logging.error("May need to run kinit again to start running jobs") 
    else:
        print("No jobs with status QUEUED")
        logging.info("No jobs with status QUEUED")
# COMPLETED
def move_batch_files_based_on_status(ssh):
    logging.info("Move batch files to their folders based off status in json file")
    # Load JSON data
    with open(cfg.json_file_path, 'r') as f:
        batch_files_data = json.load(f)

    base_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_BATCH_FILE_LOCATION).replace("\\", "/")
    base_dir = '/'.join(base_dir.split('/')[:-1])
    
    dirs_to_check = [d for d in rops.list_remote_directories(ssh, base_dir) if d.startswith('_')] # in [_QUEUED, _ERROR, _RUNNING, _COMPLETED, _FINISHED]
    # in [_QUEUED] or [_ERROR] or whatever
    for dir_name in dirs_to_check:          
        full_dir_path = os.path.join(base_dir, dir_name).replace("\\", "/") 
        # Find all files within directory
        for filename in rops.list_remote_files(ssh, full_dir_path): 
            # Get the name/path of the batch file
            batch_file_source_path = os.path.join(full_dir_path, filename).replace("\\", "/") 
            # Look through batch_files json_data
            for batch_file in batch_files_data:            
                 # Find json data entry that matches the name of the batch file we are looking at
                if batch_file['filename'] == filename:    
                    # get the status of the file from json data
                    status = batch_file['status']          
                    # If the directory doesn't match the status 
                    if dirs_to_check != '_'+status:        
                        # Move the batch file
                        rops.move_batch_file(ssh, batch_file_source_path, os.path.join(base_dir, f"_{status}").replace('\\','/'))

def evaluate_complete_directory(ssh, complete_directory):
    """
    Main method to evaluate a directory containing 'completed.txt' by running the SSH commands.
    """
    try:
        # Find the best_mIoU file and extract iteration number
        logging.info(f"Evaluating the best mIoU model placed in {complete_directory.split('/')[-1]}.")
        print(f"Evaluating the best mIoU model placed in {complete_directory.split('/')[-1]}.")
        best_mIoU_file, iteration_number = find_best_mIoU_file(ssh, complete_directory)
        if not best_mIoU_file:
            print_red(f"No best mIoU model found. Please double check in {complete_directory}")
            return

        # Run the evaluation
        model_evaluated = run_evaluation(ssh, complete_directory, best_mIoU_file)
        if model_evaluated:
            complete_directory.split('/')[-1]
            print_green(f"{complete_directory.split('/')[-1]} evaluated with {best_mIoU_file} successfully!")
            logging.info(f"{complete_directory.split('/')[-1]} evaluated with {best_mIoU_file} successfully!")
        else:
            print_red(f"{complete_directory.split('/')[-1]} was not evaluated. Double check issue with model.")
            logging.error(f"{complete_directory.split('/')[-1]} was not evaluated. Double check issue with model.")
    except Exception as e:
        print(f"An error occurred: {e}")
                
def find_best_mIoU_file(ssh, complete_directory):
    """
    Finds the .pth file with 'best_mIoU_iter' in the filename and extracts the iteration number.
    """
    # # Check if 'completed.txt' exists in the directory
    # check_command = f"ls {complete_directory}/completed.txt"
    # stdin, stdout, stderr = ssh.exec_command(check_command)
    # output = stdout.read().decode('utf-8')
    # error = stderr.read().decode('utf-8')
    # if 'No such file' in error:
    #     print(f"'completed.txt' not found in {complete_directory}. Exiting.")
    #     return None, None

    # Find the .pth file with 'best_mIoU_iter' in the name
    logging.info(f"ls {complete_directory} | grep 'best_mIoU_iter_.*\\.pth'")
    search_command = f"ls {complete_directory} | grep 'best_mIoU_iter_.*\\.pth'"
    stdin, stdout, stderr = ssh.exec_command(search_command)
    output = stdout.read().decode('utf-8')
    error = stderr.read().decode('utf-8')
    if error:
        print(f"Error finding .pth file: {error}")
        return None, None

    # Extract the filename and iteration number
    match = re.search(r"best_mIoU_iter_(\d+)\.pth", output)
    if not match:
        print("No file matching 'best_mIoU_iter_#####.pth' found.")
        return None, None

    best_mIoU_file = match.group(0)
    iteration_number = match.group(1)
    print_green(f"Best mIoU File Found: {best_mIoU_file}")
    return best_mIoU_file, iteration_number

def run_evaluation(ssh, complete_directory, best_mIoU_file):
    """
    Runs the evaluation command using the found .pth file and checks if it is successful.
    If not, retries once. If it fails again, creates 'not_evaluated.txt' in the directory.
    """
    # Construct the evaluation command
    job_work_dir_path = os.path.join(*complete_directory.split('/')[1:]).replace('\\','/')
    eval_command = (
        f"srun -G 1 --pty python tools/test.py {job_work_dir_path}/{complete_directory.split('/')[-1]}.py "
        f"{job_work_dir_path}/{best_mIoU_file} --show-dir work_dirs/{complete_directory.split('/')[-1]}/{best_mIoU_file[:-4]}_output/ --eval mIoU"
    )
    print(f"Command to run: cd {cfg.REMOTE_WORKING_PROJECT} ; {eval_command}")
    # Run the command and check if it's successful
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to the remote host
    ssh.connect(cfg.REMOTE_HOST, username=cfg.USERNAME, password=cfg.PASSWORD)
    # Open an SSH session
    logging.info("Started a shell to evaluate a model")
    session = ssh.invoke_shell()
    time.sleep(5)
    session.send(f"cd {cfg.REMOTE_WORKING_PROJECT}\n")
    time.sleep(1)
    session.send(eval_command+'\n')
    time.sleep(120)
    output = session.recv(4096).decode('utf-8')
    # print(output)
    if 'Permission denied' in output or 'error' in output.lower():
            print_red("Error running srun command.")
            logging.info("Error running srun command.")
            return False
    else:
        return True

    # logging.info(f"Executing command: cd {cfg.REMOTE_WORKING_PROJECT} ; {eval_command}")
    # stdin, stdout, stderr = ssh.exec_command(f'cd {cfg.REMOTE_WORKING_PROJECT} ; {eval_command}')
    # output = stdout.read().decode('utf-8')
    # error = stderr.read().decode('utf-8')
    # print(f"Evaluation output: {output}")
    # print(f"Error output: {error}")
    # if "Error" not in error:  # Adjust this condition based on actual success criteria
    #     print(f"Evaluation successful: {output}")
    #     return True

    # # Retry the command
    # print(f"First attempt failed: {error}. Retrying...")
    # stdin, stdout, stderr = ssh.exec_command(eval_command)
    # output = stdout.read().decode('utf-8')
    # error = stderr.read().decode('utf-8')
    # if error:
    #     print(error)
    # if output: 
    #     print(output)
    # if "Error" not in error:
    #     print(f"Evaluation successful on second attempt: {output}")
    #     return True

    # # If it fails again, create 'not_evaluated.txt'
    # print(f"Second attempt failed: {error}. Creating 'not_evaluated.txt'...")
    # create_file_command = f"echo 'Evaluation failed' > {complete_directory}/not_evaluated.txt"
    # stdin, stdout, stderr = ssh.exec_command(create_file_command)
    # output = stdout.read().decode('utf-8')
    # error = stderr.read().decode('utf-8')
    # return False

def log_extraction(ssh):
    logging.info("Extracting logs for models that have completed training")
    try:
        # Find directories that have the completed.txt file indicating that training is done
        project_work_dir = f'{cfg.REMOTE_WORKING_PROJECT}/{cfg.REMOTE_WORK_DIR}'
        # print(f'Project_Work_dir:\t{project_work_dir}')

# LOOKING FOR COMPLETED.TXT FILE, NOT CHECKING STATUS MAY NEED TO CHECK STATUS TO MAKE 
# SURE IT IS MARKED AS COMPLETED BEFORE STARTING PROCESSING
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
# -----------------------------------------------                    
                    # Example usage (assuming cfg is correctly set up)
                    evaluate_complete_directory(ssh, directory)
# -----------------------------------------------
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
                        print_green(f"Setting status of job in {directory.split('/')[-1]} to FINISHED")
                        json_utils.set_status_of_batch_file("FINISHED", working_directory=directory.split('/')[-1])
                else:
                    logging.error(f'No JSON files were found in this directory: {directory}')
                    print_red(f'No JSON files were found in this directory: {directory}')
        else:
            logging.error(f"{cfg.COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
            print_red(f"{cfg.COMPLETED_MARKER_FILE} not found in directory {project_work_dir}")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        print(f"An error occured: {str(e)}")

def check_and_move_files(ssh):
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
            move_directories(ssh, directories)
            print(f"Would move these directories {directories}")
        else:
            print_red("No directories found to move")
            logging.info("No directories found to move.")
    else:
        print_green("Storage usage is within limits.")
        logging.info("Storage usage is within limits.")

def run_every_hour(ssh):

    json_utils.update_json_new(ssh)
      
    jobs=rops.get_squeue_jobs(ssh)
    check_batch_files(ssh, jobs)

    last_status_counts = json_utils.update_json_new(ssh)
    print(f"Number of jobs running on Remote Server: {len(jobs)}")
    if len(jobs) < cfg.JOB_THRESHOLD or last_status_counts[3] < cfg.JOB_THRESHOLD:
        print_blue("Fewer jobs than threshold detected. Run this line 3")
        run_sbatch(ssh)
        # move_batch_files_based_on_status(ssh)    
    # Look and extract logs for jobs that are completed. 
    json_utils.update_json_new(ssh)

    log_extraction(ssh)
    move_batch_files_based_on_status(ssh)
    check_and_move_files(ssh)
    
    
def run_every_six_hours():
    rops.ssh_kinit_loop(3)

def main():
    # TODO FIX STATUS UPDATES FOR RUNNING MODELS... We might not be clearing lists to queue and sbatch models properly
    run_counter = 0
    sleep_counter_seconds = 30
    ssh = rops.connect_ssh(remote_host=cfg.REMOTE_HOST, username=cfg.USERNAME, password=cfg.PASSWORD)
    json_utils.create_json(ssh)
    schedule.every(1).minutes.do(run_every_hour, ssh)
    # schedule.every(2).minute.do(run_every_six_hours)
    run_every_hour(ssh)
    # run_every_six_hours()

    try:
        while True:
            run_counter+=1
            # Run all pending scheduled tasks
            schedule.run_pending()
            print(f"Run counter: {run_counter}. Sleeping for {sleep_counter_seconds} seconds.")
            time.sleep(sleep_counter_seconds)
            
            
    except Exception as e:
        print(e)
        logging.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        ssh.close()
    

if __name__ == '__main__':
    main()
