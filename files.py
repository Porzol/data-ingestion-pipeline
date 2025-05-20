import re
from config import directories
from pathlib import Path
import pandas as pd
import json
import pickle
import numpy as np
from datetime import datetime


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        elif isinstance(obj, np.ndarray):
            return {"__type__": "ndarray", "value": obj.tolist()}
        elif isinstance(obj, np.generic):  # for np.float32, etc.
            return obj.item()
        return super().default(obj)


def write_dict_as_pickle(data:dict, output_directory:Path, name:str) -> None:
    #  Prevent directory issues
    name = re.sub("/", "_", name)
    with open(output_directory / f"{name}.pkl", 'wb') as f:
        pickle.dump(data, f)

def write_dict_as_json(data, output_directory:Path, name:str):
    name = re.sub("/", "_", name)
    
    try:
        with open(output_directory / f"{name}.json", 'w') as f:
            json.dump(data, f, indent = 4, cls = CustomJSONEncoder)
    except Exception as exception:
        print(f"Something went wrong when trying to save file: {name}\n{data}\n{exception}")

def write_dataframe_as_pickle(df:pd.DataFrame, output_directory:Path, name:str):
    name = re.sub("/", "_", name)
    
    try:
        df.to_pickle(output_directory / f"{name}.pkl")
    except Exception as exception:
        print(f"Something went wrong when trying to save file: {name} \n {exception}")

def initialize_directories(directories:list[Path]):
    for directory in directories:
        directory.mkdir(parents = True, exist_ok = True)

def load_textfile(directory:Path):
    with open(directory, 'r', encoding = 'utf-8') as f:
        return f.read()