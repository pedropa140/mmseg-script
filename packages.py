import ast
import importlib.metadata
import os
import glob

GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def extract_imports_from_file(file_path):
    with open(file_path, 'r') as file:
        tree = ast.parse(file.read(), filename=file_path)

    imported_modules = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_modules.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            imported_modules.add(node.module.split('.')[0])
    
    return imported_modules

def get_third_party_modules(imported_modules):
    installed_packages = {}
    
    for pkg in importlib.metadata.distributions():
        package_name = pkg.metadata['Name'].lower()
        package_version = pkg.version
        installed_packages[package_name] = package_version

    third_party_modules = {}

    for module in imported_modules:
        if module in installed_packages:
            third_party_modules[module] = installed_packages[module]

    return third_party_modules

def main():
    try:
        python_files = glob.glob('**/*.py', recursive=True)
        
        if not python_files:
            print(f"{RED}No Python files found.{RESET}")
            return
        
        all_imported_modules = set()
        
        for python_file in python_files:
            imported_modules = extract_imports_from_file(python_file)
            all_imported_modules.update(imported_modules)
        
        third_party_modules = get_third_party_modules(all_imported_modules)
        
        if not third_party_modules:
            print(f"{RED}No third-party modules found.{RESET}")
            return

        with open('requirements.txt', 'w') as req_file:
            for module, version in third_party_modules.items():
                if version:
                    req_file.write(f"{module}=={version}\n")
                else:
                    req_file.write(f"{module}\n")
                
                print(f"{GREEN}Added: {module}=={version}{RESET}")

        print(f"{GREEN}Requirements file created successfully!{RESET}")
    except Exception as e:
        print(f"{RED}An error occurred: {e}{RESET}")

if __name__ == "__main__":
    main()
