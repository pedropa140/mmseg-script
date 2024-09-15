from dotenv import load_dotenv
import logging
import os

load_dotenv()

# Setup logging
logging.basicConfig(filename='storage_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

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
'''
# Manual Configuration

global THRESHOLD
THRESHOLD = 45  # Set your threshold percentage

global JOB_THRESHOLD
JOB_THRESHOLD = 5

global REMOTE_BASE_PATH 
global REMOTE_WORKING_PROJECT 
global REMOTE_WORK_DIR  
global REMOTE_BATCH_FILE_LOCATION 
global REMOTE_BATCH_FILE_PATH 

REMOTE_BASE_PATH = '/common/home/bn155'
REMOTE_WORKING_PROJECT = 'mmseg-personal'
REMOTE_WORK_DIR = 'work_dirs'
REMOTE_BATCH_FILE_LOCATION = 'tools/batch_files/_QUEUED'
REMOTE_BATCH_FILE_PATH = 'mmseg-personal/tools/batch_files/_QUEUED'

global json_file_path
json_file_path = 'batch_files.json'

# ----- Getenv variables -----

PLINK_PATH=os.getenv('plink_path')
PSCP_PATH=os.getenv('pscp_path')
global windows
global linux
if PLINK_PATH != '' and PSCP_PATH != '':
    print(f"Found path to plink.exe: {PLINK_PATH}")
    print(f"Found path to pscp.exe: {PSCP_PATH}")
    print("Assuming Operating system to be windows!")
    windows=True
    linux=False
else:
    print("Did not find plink.exe or pscp.exe paths, assuming Operating System to be: Linux")
    windows=False
    linux=True

global USERNAME        
USERNAME = os.getenv('netid')
if USERNAME is None:
    print("Username not found. Did you create a .env file?")
    logging.error("Username not found. Did you create a .env file?")


global LOCAL_PATH
LOCAL_PATH = os.getenv('local_path')

if LOCAL_PATH == '':
    print("Local path not found. Check path and .env file")
if os.path.exists(LOCAL_PATH):
    print(f"Local path found!: {LOCAL_PATH}")
else:
    print("The local path provided does not exist. Please check your path before proceeding.")
    logging.error("The local path provided does not exist. Please check your path before proceeding.")


global PASSWORD    
PASSWORD = os.getenv('password')

global REMOTE_HOST
REMOTE_HOST = os.getenv('remote_host')
if REMOTE_HOST is None:
    print("Remote Host not found. Did you create a .env file?")
    logging.error("Remote Host not found. Did you create a .env file?")

global FINISHED_MARKER_FILE
FINISHED_MARKER_FILE = os.getenv('finished_marker_file')  # The file that indicates the directory should be moved

global COMPLETED_MARKER_FILE
COMPLETED_MARKER_FILE = os.getenv('completed_marker_file') # Indicates that files are done training, and log_extraction can be run
