import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from functions import m_data

#set Machine for wich you want to calculate dataoutput
set_machine = 'DLRA'

path_to_machine_data = f'data/{set_machine}'

folder_paths = m_data.read_folder_paths(path_to_machine_data)

for path in folder_paths:
    if 'datarecorder' in path.lower():
        folder_path_datarecorder = path
        break

data_raw_datarecorder = m_data.read_datarecorder_file(folder_path_datarecorder)

for path in folder_paths:
    if 'protocoll_series' in path.lower():
        folder_path_protocoll = path
    if 'lascar' in path.lower():
        folder_path_lascar = path


data_protocoll = m_data.read_protocoll_file(folder_path_protocoll)

data_timestamps_filtered_datarecorder = m_data.compare_datatimestamps_recorder_protocoll(data_raw_datarecorder, data_protocoll)

data_timestamps_filtered_datarecorder.to_csv(f'output/{set_machine}_data_timestamps_filtered_datarecorder.csv', index=False)

data_usb_logger = m_data.read_lascar_file(folder_path_lascar)

data_usb_logger_filtered = m_data.compare_datatimestamps_lascar_protocoll(data_usb_logger, data_protocoll)

data_usb_logger_filtered.to_csv(f'output/{set_machine}_data_usb_logger.csv', index=False)
