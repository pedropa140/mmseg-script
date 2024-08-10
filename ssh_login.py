import subprocess
import time
import os
from dotenv import load_dotenv

# Define color codes
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

load_dotenv()

def change_directory(target_dir):
    current_dir = os.getcwd()
    print(f"Current Directory: {current_dir}")

    new_dir = os.path.join(current_dir, target_dir)
    try:
        os.chdir(new_dir)
        print(GREEN + f"Changed to Directory: {os.getcwd()}" + RESET)
    except FileNotFoundError:
        print(RED + f"Error: The directory '{new_dir}' does not exist." + RESET)
    except Exception as e:
        print(RED + f"An error occurred: {str(e)}" + RESET)

# TODO - Create a script to run batch file
def run_batch_file():
    return NotImplementedError

# TODO - Need to check if training is done
def check_training_if_done():
    return NotImplementedError

# TODO - Create log extraction file
def log_extraction():
    return NotImplementedError

# TODO - create file to mark completed jobs
def check_completed_jobs():
    return NotImplementedError

# TODO - move completed work directory to local PC
def transfer_to_local():
    return NotImplementedError

if __name__ == "__main__":
    change_directory("../mmseg-personal")
