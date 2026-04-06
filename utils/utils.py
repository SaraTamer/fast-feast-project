import os

def get_file_extension(file_path: str) -> str:
    return os.path.splitext(file_path)[1].lower().replace(".", "")


def get_table_name(file_path: str) -> str:
    """Extract table name from file path"""
    filename = os.path.basename(file_path)
    table_name = filename.split('.')[0]
    return table_name