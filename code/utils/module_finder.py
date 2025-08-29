import ast
import os

def find_imports_in_file(file_path, module_name):

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            node = ast.parse(file.read(), filename=file_path)
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.name == module_name:
                        return True
            elif isinstance(stmt, ast.ImportFrom):
                if stmt.module and stmt.module.split('.')[0] == module_name:
                    return True
    except (SyntaxError, UnicodeDecodeError):
        pass
    return False

def find_files_with_import(root_dir, module_name):
    matches = []
    for root, _, files in os.walk(root_dir):
        for filename in files:
            if filename.endswith('.py'):
                full_path = os.path.join(root, filename)
                if find_imports_in_file(full_path, module_name):
                    matches.append(full_path)
    return matches

# Example usage:
if __name__ == "__main__":
    root_directory = "/home/imchugh/Code/TERN-EP_data_pipeline/code"  # or specify your project's root
    target_module = "paths_manager"  # change to the module you're looking for
    results = find_files_with_import(root_directory, target_module)

    print(f"Python files importing '{target_module}':")
    for filepath in results:
        print(filepath)

