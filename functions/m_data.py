import os
import pandas as pd
from pathlib import Path
from pandas import DataFrame

def read_folder_paths(path: str) -> list[str]:
    """Reads all folder paths in a given directory.

    Args:
        path (str): path to folder with certain data files/folders
    Returns:
        list[str]: list of folder paths
    """

    folder_paths = [
        os.path.join(path, folder_name)
        for folder_name in os.listdir(path)
        if os.path.isdir(os.path.join(path, folder_name))
    ]
    return folder_paths

def read_datarecorder_file(folder_path_datarecorder: str) -> DataFrame:
    """Read in all csv-files from folder as a single DataFrame

    Args:
        folder_path_datarecorder (str): path to folder of datarecorder files

    Returns:
        DataFrame: concatenated DataFrame of all CSV timelines
    """
    folder = Path(folder_path_datarecorder)

    dfs = []

    for csv_file in sorted(folder.glob("*.csv")):
        df = pd.read_csv(
            csv_file,
            parse_dates=["Timestamp"],  # Timestamp as datetime
            low_memory=False
        )

        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values("Timestamp").reset_index(drop=True)

    return combined

def read_protocoll_file(folder_path_protocoll: str) -> DataFrame:
    """Read all CSV files from a protocol folder and return a single DataFrame

    Args:
        folder_path_protocoll (str): Path to the folder containing protocol CSV files

    Returns:
        DataFrame: Concatenated DataFrame of all protocol CSV files
    """
    folder = Path(folder_path_protocoll)

    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Invalid folder path: {folder_path_protocoll}")

    dfs = []

    for csv_file in sorted(folder.glob("*.csv")):

        df = pd.read_csv(
            csv_file,
            sep=';',
            low_memory=False
        )

        #convert timestamps seperatly
        for col in ['start_time', 'end_time']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M')

        dfs.append(df)

    if not dfs:
        raise ValueError("No CSV files found in the protocol folder.")

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values('start_time').reset_index(drop=True)

    return combined

def compare_datatimestamps_recorder_protocoll(df_datarecorder: DataFrame, df_protocoll: DataFrame) -> DataFrame:
    """Compare datarecorder Timestamps to protocoll Timestamps and
       only keep datarecorder rows that fall within any protocoll start/end interval.

    Args:
        df_datarecorder (DataFrame): DataFrame of datarecorder file, must have 'Timestamp' column
        df_protocoll (DataFrame): DataFrame of protocoll file, must have 'start_time' and 'end_time'

    Returns:
        DataFrame: Filtered version of df_datarecorder
    """
    # Sicherstellen, dass Timestamp-Spalten datetime sind
    df_datarecorder = df_datarecorder.copy()
    df_datarecorder['Timestamp'] = pd.to_datetime(df_datarecorder['Timestamp'])
    df_protocoll['start_time'] = pd.to_datetime(df_protocoll['start_time'])
    df_protocoll['end_time'] = pd.to_datetime(df_protocoll['end_time'])

    # Leeres Filter-Array initialisieren
    mask = pd.Series(False, index=df_datarecorder.index)

    # Für jedes Protokoll-Intervall prüfen, welche Timestamps dazugehören
    for _, row in df_protocoll.iterrows():
        mask |= (df_datarecorder['Timestamp'] >= row['start_time']) & (df_datarecorder['Timestamp'] <= row['end_time'])

    # Gefiltertes DataFrame zurückgeben
    return df_datarecorder[mask].reset_index(drop=True)