import paramiko
import logging
import os
import config as cfg

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_red(text):
    print(f"\033[91m{text}\033[0m")

def print_blue(text):
    print(f"\033[38;2;50;128;128m{text}\033[0m")

def connect_ssh(remote_host, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # PASSWORD = getpass("Enter your SSH password: ")
    try:
        ssh.connect(hostname=remote_host, username=username, password=password)
        print_green("Successfully connected to SSH.")
        logging.info("Successfully connected to SSH.")
        return ssh
    except Exception as e:
        print_red(f"Failed to connect to SSH: {e}")
        logging.error(f"Failed to connect to SSH: {e}")
        return None
    
def check_remote_file_exists(ssh, path):
    logging.info(f"Check if remote file exists in: {path})")
    logging.info(f"Executing: 'if [ -f {path} ]; then echo 'exists'; fi'")
    stdin, stdout, stderr = ssh.exec_command(f'if [ -f {path} ]; then echo "exists"; fi')
    result = stdout.read().decode().strip()
    # print(f"{path}: {result}")
    logging.info(f"{path}: {result}")
    return result == "exists"

def check_and_update_status(ssh, job, status_file, new_status, source_directory, target_directory):
    work_dir = os.path.join(cfg.REMOTE_WORKING_PROJECT, cfg.REMOTE_WORK_DIR, job['working_directory'])
    command = f"find {work_dir} -maxdepth 1 -name {status_file}"
    stdin, stdout, stderr = ssh.exec_command(command)
    if stdout.read().strip():  # If the status file exists
        logging.info(f"Changing {job['job_name']} status to {new_status}")
        job['status'] = new_status
        source_location = {os.path.join(source_directory, job['filename']).replace('\\', '/')}
        dest_location = {os.path.join(target_directory, job['filename']).replace('\\', '/')}
        move_command = f'mv {source_location} {dest_location}'
        logging.info(f"Executing move command: {move_command}")
        stdin, stdout, stderr = ssh.exec_command(move_command)
        stderr_output = stderr.read().decode()
        if stderr_output:
            logging.error(f"Error moving file: {stderr_output}")
    return job

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

def find_sbatch_files_from_directory(ssh):
    logging.info("Finding sbatch files from directories")
    # Find all the batch files within the specified directory from home directory
    logging.info(f"Executing: 'find {cfg.REMOTE_BATCH_FILE_PATH} -name '*.batch''")
    stdin, stdout, stderr = ssh.exec_command(f'find {cfg.REMOTE_BATCH_FILE_PATH} -name "*.batch"')
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

def list_remote_files(ssh, path):
    logging.info(f"list remote files in: {path})")
    logging.info(f"Executing command: 'ls {path}'")
    stdin, stdout, stderr = ssh.exec_command(f'ls {path}')
    files = stdout.read().decode().strip().splitlines()
    logging.info(f"Files found: {files}")
    return files

def list_remote_directories(ssh, path):
    logging.info(f"list remote directories in: {path})")
    logging.info(f"Executing: 'ls -d {path}/*/'")
    stdin, stdout, stderr = ssh.exec_command(f'ls -d {path}/*/')
    dirs = stdout.read().decode().strip().splitlines()
    return [os.path.basename(d.rstrip('/')) for d in dirs]

def get_job_name_from_batch_file(ssh, batch_file_path):
    logging.info(f"Executing: cat {batch_file_path}")
    stdin, stdout, stderr = ssh.exec_command(f'cat {batch_file_path}')
    for line in stdout:
        if line.startswith("#SBATCH --job-name="):
            logging.info(f"Found job-name: ({line.split('=')[-1].strip()}) from {batch_file_path}")
            return line.split("=")[-1].strip()
    return None

def get_python_file_name_from_batch_file(ssh, batch_file_path, working_project=cfg.REMOTE_WORKING_PROJECT):
    logging.info(f"Get folder name from batch file: {batch_file_path})")
    stdin, stdout, stderr = ssh.exec_command(f'cat {batch_file_path}')
    
    for line in stdout:
        line = line.strip()
        if f"python3 ~/{working_project}/tools/train.py" in line:
            working_line = line.replace(f'python3 ~/{working_project}/tools/train.py ~', '').replace('.py', '').split("/")
            working_length = len(working_line)
            working_directory = working_line[working_length - 1]
            logging.info(f"Found name of folder in batch file: {working_directory}")
            return working_directory
    return None

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

