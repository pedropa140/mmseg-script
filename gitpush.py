import subprocess

GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def git_push(branch="master"):
    try:
        subprocess.run(["git", "add", "."], check=True)
        
        commit_message = input("Enter your commit message: ")
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push", "origin", branch], check=True)

        print(f"{GREEN}git push successful!{RESET}")

    except subprocess.CalledProcessError as e:
        print(f"{RED}Error: {e}{RESET}")
        print(f"{RED}git push failed.{RESET}")
        subprocess.run(["git", "reset"], check=True)
        print(f"{RED}git reset.{RESET}")

if __name__ == "__main__":
    git_push(input("Enter Branch Name:\t"))
