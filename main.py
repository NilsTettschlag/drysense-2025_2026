import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from functions import m_data

#set Machine for wich you want to calculate dataoutput
set_machine = 'DLRA'  # Options: 'OCEAN', 'DLRA'

# Define path to set machine data
path_to_machine_data = f'data/{set_machine}'

# Read folder paths in machine data directory
folder_paths = m_data.read_folder_paths(path_to_machine_data)

# Identify datarecorder folder path
for path in folder_paths:
    if 'datarecorder' in path.lower():
        folder_path_datarecorder = path
        break

# Read raw datarecorder data
data_raw_datarecorder = m_data.read_datarecorder_file(folder_path_datarecorder)

# Identify protocol and lascar folder paths
for path in folder_paths:
    if 'protocoll_series' in path.lower():
        folder_path_protocoll = path
    if 'lascar' in path.lower():
        folder_path_lascar = path

# Read protocol data
data_protocoll = m_data.read_protocoll_file(folder_path_protocoll)

# filter datarecorder data based on protocol timestamps
data_timestamps_filtered_datarecorder = m_data.compare_datatimestamps_recorder_protocoll(data_raw_datarecorder, data_protocoll, set_machine)

# Save outputs of datarecorder data
data_timestamps_filtered_datarecorder.to_csv(f'output/{set_machine}/data_timestamps_filtered_datarecorder.csv', index=False)

# Read lascar data
data_usb_logger = m_data.read_lascar_file(folder_path_lascar)

# filter lascar data based on protocol timestamps
data_usb_logger_filtered = m_data.compare_datatimestamps_lascar_protocoll(data_usb_logger, data_protocoll)

# Save outputs of lascar data
data_usb_logger_filtered.to_csv(f'output/{set_machine}/data_usb_logger.csv', index=False)

# read dryness data and add dryness values to datarecorder data
path_dryness_folder = f'data/{set_machine}/protocoll_dryness'

df_dryness = m_data.read_dryness_data(path_dryness_folder)

data_with_dryness = m_data.add_dryness_values(data_timestamps_filtered_datarecorder, df_dryness)
