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

def compare_datatimestamps_recorder_protocoll(df_datarecorder: DataFrame, df_protocoll: DataFrame, machine: str) -> DataFrame:
    """Compare datarecorder Timestamps to protocoll Timestamps and
       only keep datarecorder rows that fall within any protocoll start/end interval.
       Also adds the corresponding t_duration from protocoll.

    Args:
        df_datarecorder (DataFrame): DataFrame of datarecorder file, must have 'Timestamp' column
        df_protocoll (DataFrame): DataFrame of protocoll file, must have 'start_time' and 'end_time'

    Returns:
        DataFrame: Filtered version of df_datarecorder with added t_duration column
    """
    # Sicherstellen, dass Timestamp-Spalten datetime sind
    df_datarecorder = df_datarecorder.copy()
    df_datarecorder['Timestamp'] = pd.to_datetime(df_datarecorder['Timestamp'])
    df_protocoll['start_time'] = pd.to_datetime(df_protocoll['start_time'])
    df_protocoll['end_time'] = pd.to_datetime(df_protocoll['end_time'])

    # Leeres Filter-Array initialisieren
    mask = pd.Series(False, index=df_datarecorder.index)
    if machine == 'OCEAN':
        t_duration_list = pd.Series(None, index=df_datarecorder.index)
    else:
        if machine == 'DLRA':
            rotation_speed_UL = pd.Series(None, index=df_datarecorder.index)
            temperature_drying = pd.Series(None, index=df_datarecorder.index)


    # Für jedes Protokoll-Intervall prüfen, welche Timestamps dazugehören
    for _, row in df_protocoll.iterrows():
        interval_mask = (df_datarecorder['Timestamp'] >= row['start_time']) & (df_datarecorder['Timestamp'] <= row['end_time'])
        mask |= interval_mask
        if machine == 'OCEAN':
            t_duration_list[interval_mask] = row['t_duration']
        else:
            if machine == 'DLRA':
                rotation_speed_UL[interval_mask] = row['n_UL']
                temperature_drying[interval_mask] = row['T_drying']

    # Gefiltertes DataFrame mit t_duration zurückgeben
    result = df_datarecorder[mask].reset_index(drop=True)
    if machine == 'OCEAN':
        result['t_duration'] = t_duration_list[mask].values
    else:
        if machine == 'DLRA':
            result['n_UL'] = rotation_speed_UL[mask].values
            result['T_drying'] = temperature_drying[mask].values
    return result

def read_lascar_file(folder_path_lascar: str) -> DataFrame:
    """Read in all csv-files from lascar folder as a single DataFrame

    Args:
        folder_path_lascar (str): path to folder of lascar files

    Returns:
        DataFrame: Concatenated DataFrame of all Lascar USB logger CSV files
    """

    folder = Path(folder_path_lascar)

    dfs = []

    for csv_file in sorted(folder.glob("*.csv")):
        df = pd.read_csv(
            csv_file,
            sep=";",
            encoding="cp1252",              # wichtig wegen °C
            decimal=",",                     # deutsches Dezimaltrennzeichen
            names=["Index", "Time", "Temperature_C", "Serial_Number"],
            header=0,                        # erste Zeile ist Header
            parse_dates=["Time"],
            dayfirst=True,                   # DD.MM.YYYY
            low_memory=False,
            dtype={"Serial_Number": "string"}
        )
        df = df.drop(columns=["Index"])
        df["Serial_Number"] = df["Serial_Number"].ffill()


        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values("Time").reset_index(drop=True)

    # Pivot: jede Seriennummer bekommt eine eigene Spalte
    df_wide = combined.pivot(index="Time", columns="Serial_Number", values="Temperature_C")

    # Spaltennamen schöner machen
    df_wide.columns = [f"Sensor_{int(c)}" for c in df_wide.columns]

    return df_wide.reset_index()

def compare_datatimestamps_lascar_protocoll(df_lascar: DataFrame, df_protocoll: DataFrame) -> DataFrame:
    """Compare lascar Timestamps to protocoll Timestamps and
       only keep lascar rows that fall within any protocoll start/end interval.

    Args:
        df_lascar (DataFrame): DataFrame of lascar file, must have 'Timestamp' column
        df_protocoll (DataFrame): DataFrame of protocoll file, must have 'start_time' and 'end_time'

    Returns:
        DataFrame: Filtered version of df_lascar
    """
    df_lascar = df_lascar.sort_values('Time')
    df_protocoll = df_protocoll.sort_values('start_time')

    df_merged = pd.merge_asof(
        df_lascar,
        df_protocoll,
        left_on='Time',
        right_on='start_time',
        direction='backward'
    )

    mask = df_merged['Time'] <= df_merged['end_time']

    # Alle Spalten von df_lascar behalten, gefiltert nach Intervall
    df_filtered = df_merged.loc[mask, df_lascar.columns].reset_index(drop=True)

    return df_filtered

def read_dryness_data(folder_path_dryness_data: str) -> DataFrame:
    """Read external dryness data file

    Args:
        folder_path_dryness_data (str): Path to the dryness data folder
    Returns:
        DataFrame: DataFrame containing dryness data
    """

    folder = Path(folder_path_dryness_data)

    for csv_file in sorted(folder.glob("*.csv")):
        df_dryness = pd.read_csv(
            csv_file,
            sep=';',
            encoding="cp1252",
            low_memory=False
        )

    # only keep spalten that have enties in every cell
    df_dryness = df_dryness.dropna(axis=0, how='any')

    # drop row m_before, m_after, m_diff and n_set --> unecessary for adding dryness values
    df_dryness = df_dryness.drop(columns=['m_before', 'm_after', 'm_diff', 'n_set'], errors='ignore')

    # reset index
    df_dryness = df_dryness.reset_index(drop=True)

    return df_dryness

def add_dryness_values(df_datarecorder: DataFrame, df_dryness_data: DataFrame) -> DataFrame:
    """Add dryness values to datarecorder DataFrame based on external dryness data file.

    Args:
        df_datarecorder (DataFrame): DataFrame containing filtered datarecorder Data and Protocoll data
        df_dryness_data (DataFrame): DataFrame containing external dryness data

    Returns:
        DataFrame: DataFrame with added dryness values
    """

    return df_datarecorder  