import re
import logging
from pathlib import Path
import multiprocessing as mu

import pandas as pd
import numpy as np
from tqdm import tqdm
from pandarallel import pandarallel

from config import etl_config

pandarallel.initialize(progress_bar = True)
logger = logging.getLogger(__name__)

def load_excel_data(directory:Path) -> pd.DataFrame:
    dataframes = list()

    excel_files = list()
    excel_files.extend(directory.glob("*.xlsx"))
    excel_files.extend(directory.glob("*.xls"))

    with mu.Pool(processes = mu.cpu_count(), maxtasksperchild = 2) as pool:
        dataframes = list(tqdm(pool.imap(pd.read_excel, excel_files, chunksize = 5), total = len(excel_files)))

    return pd.concat(dataframes, ignore_index = True)

def filter_duplicate_chatlogs(df: pd.DataFrame):
    df["key"] = df[["Sender", "Creator", "Sent to", "Sent time", "Sent date"]].astype(str).agg('_'.join, axis=1)
    df = df.drop_duplicates(subset="key")
    df = df.drop(columns="key")

    df = df[df["Sent to"] != "Deleted user"]
    
    return df

def remove_html_tags(text:str) -> str:
    HTML_TAGS_REGEX = r"<\/?.+?>"
    return re.sub(HTML_TAGS_REGEX, '', text)

def manual_spellcheck(word:str, substitutions:dict = etl_config.MANUAL_SUBSTITUTION_LIST) -> str:
    if word in substitutions:
        return substitutions[word]
    else:
        return word

def unstretch_word(word:str) -> str:
    """
    Some words we use may be stretched to make an emphasis on something.
    Eg: Her tits are huuuuuugggeeeeee
    This function takes care of that.
    """
    return re.sub(r"(.)\1{2,}", r"\1", word)

def extract_tip_value(text:str) -> float:
    TIP_REGEX = r'^I sent you a \$(\d+(\.\d{2})?) tip$'

    tip_regex_matches = re.search(TIP_REGEX, text)

    if tip_regex_matches is not None:
        tip_amount = str(tip_regex_matches.group(1))
        return float(tip_amount)
    else:

        return 0.0

def format_and_clean_dataframe(chatlogs:pd.DataFrame) -> pd.DataFrame:
    NEW_COLUMN_NAMES = {
        "Sender":"chatter_name",
        "Creator":"model_name",
        "Sent to":"fan_id",
        "Fans Message":"fan_message",
        "Creator Message":"chatter_message",
        "Purchased":"purchased",
        "Price":"price",
    }
    
    chatlogs = chatlogs.rename(columns = NEW_COLUMN_NAMES)

    chatlogs["fan_message"] = chatlogs["fan_message"].fillna('')
    chatlogs["chatter_message"] = chatlogs["chatter_message"].fillna('')

    chatlogs["fan_message"] = chatlogs["fan_message"].astype(str)
    chatlogs["chatter_message"] = chatlogs["chatter_message"].astype(str)

    chatlogs["fan_message"] = chatlogs["fan_message"].parallel_apply(remove_html_tags)
    chatlogs["chatter_message"] = chatlogs["chatter_message"].parallel_apply(remove_html_tags)
    
    chatlogs["purchased"] = chatlogs["purchased"].map({"yes": True, 'no': False})
    chatlogs["purchased"] = chatlogs["purchased"].astype(bool)

    chatlogs["price"] = chatlogs["price"].astype(float)

    chatlogs["datetime"] = pd.to_datetime(chatlogs["Sent date"] + ' ' + chatlogs["Sent time"])

    chatlogs["tips"] = chatlogs["fan_message"].parallel_apply(extract_tip_value)

    chatlogs['revenue'] = chatlogs.apply(lambda row: row['price'] + row['tips'] if row['purchased'] else row['tips'], axis=1)

    chatlogs = chatlogs.drop(columns=["Sent time", "Sent date", "Status", "Replay time"])

    return chatlogs

def get_documents(df:pd.DataFrame):
    return [{
            "chatter_message": row["chatter_message"],
            "chatter_name": row["chatter_name"], 
            "datetime": row["datetime"], 
            "fan_message": row["fan_message"], 
            "fan_id": row["fan_id"], 
            "model_name": row["model_name"], 
            "price": row["price"], 
            "purchased": row["purchased"], 
            "revenue": row["revenue"], 
            "tips": row["tips"], 
        } for _, row in df.iterrows()]
