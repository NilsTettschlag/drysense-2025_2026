import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from functions import m_data

path_to_data = 'data'

folder_paths = m_data.read_folder_paths(path_to_data)

print(f"Folder paths found: {folder_paths}")