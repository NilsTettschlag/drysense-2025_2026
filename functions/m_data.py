import os

def read_folder_paths(path: str) -> list[str]:
    """Reads all folder paths in a given directory.

    Args:
        path (str): path to folder with certain data files
    Returns:
        list[str]: list of folder paths
    """

    folder_paths = [
        os.path.join(path, folder_name)
        for folder_name in os.listdir(path)
        if os.path.isdir(os.path.join(path, folder_name))
    ]
    return folder_paths