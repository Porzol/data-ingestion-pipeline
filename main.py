import files
import etl
import chunk
from config import directories, main_config, chunk_config, elastic_config
import pandas as pd 
import hashlib
import embed
import elastic 
import logging
import multiprocessing as mu
from tqdm import tqdm
import os

def main():
    files.initialize_directories(directories.DIRS_TO_INIT)

    if main_config.SKIP_ETL_PROCESS:
        chatlogs_df = pd.read_pickle(directories.COMBINED_DATA / "all_chatlogs.pkl")
    else:
        chatlogs_df = etl.load_excel_data(directories.FULL_DATASET)
        chatlogs_df = etl.filter_duplicate_chatlogs(chatlogs_df)
        chatlogs_df = etl.format_and_clean_dataframe(chatlogs_df)

    if main_config.CUT_DOWN_CHATLOGS_SIZE and main_config.MINIMUM_REVENUE_FROM_USER > 0:
        user_totals = chatlogs_df.groupby('fan_id')['revenue'].sum()
        big_spenders = user_totals[user_totals >= main_config.MINIMUM_REVENUE_FROM_USER].index
        chatlogs_df = chatlogs_df[chatlogs_df['fan_id'].isin(big_spenders)]

    if main_config.SAVE_DATA_TO_DISK:
        files.write_dataframe_as_pickle(chatlogs_df, directories.COMBINED_DATA, "all_chatlogs")

    # Displays the surface statistics of the chatlogs (for verifying if we got all the data)
    temp_revenue_sum = chatlogs_df["revenue"].sum()
    temp_row_count = chatlogs_df.shape[0]
    temp_unique_name_count = chatlogs_df['fan_id'].nunique()
    print(f"REVENUE IN TOTAL: {temp_revenue_sum}\nCHATLOGS:{chatlogs_df}\nNUMBER OF FANS:{temp_unique_name_count}")

    # May be useful for fan profiling in the future
    if main_config.SAVE_DATA_TO_DISK:
        fans_chatlogs = chatlogs_df.groupby("fan_id")
        for name, df in fans_chatlogs:
            files.write_dataframe_as_pickle(df, directories.INDIVIDUAL_DATA, name)

    groups = chunk.groupby_participants(chatlogs_df)
    groups = [chunk.reshape_chatlog(group) for group in groups]
    
    conversations:list[dict] = list()

    with mu.Pool(processes = mu.cpu_count(), maxtasksperchild = 2) as pool:
        convos = list(tqdm(pool.imap(chunk.chunk_conversations, groups, chunksize = 2), total = len(groups)))

        for item in convos:
            conversations.extend(item)

    if main_config.SAVE_DATA_TO_DISK:
        for convo in conversations:
            files.write_dict_as_pickle(convo, directories.CONVERSATIONAL_DATA, f"{convo['fan_id']}-{convo['model_id']}-{convo['conversation_number']}")
            files.write_dict_as_json(convo, directories.JSON_CONVERSATIONS, f"{convo['fan_id']}-{convo['model_id']}-{convo['conversation_number']}")

    if main_config.ENABLE_ELASTIC_PROCESS:
        ESKEY = "PUT YOUR ELASTIC KEY HERE"
        elastic_client = elastic.get_elatic_client(elastic_config.CLOUD_URL, ESKEY)
        elastic.upload_documents(conversations, elastic_client, elastic_config.CONVERSATIONS_INDEX_NAME, 120)

if __name__ == "__main__":
    main()