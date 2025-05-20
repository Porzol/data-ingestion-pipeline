import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import timedelta
from config import chunk_config
import multiprocessing as mu
import logging
from math import isnan

logger = logging.getLogger(__name__)

def groupby_participants(df: pd.DataFrame):
    df["conversation-key"] = df.apply(lambda row: f"{row['fan_id']}-{row['model_name']}", axis=1)
    dataframes = [data for name, data in df.groupby("conversation-key")]
    return dataframes

def reshape_chatlog(df: pd.DataFrame):
    messages = list()
    for _, row in df.iterrows():
        fan = row["fan_id"]
        model = row["model_name"]
        time = row["datetime"]
        chatter_time = time + timedelta(seconds=1)

        if pd.notna(row["fan_message"]) and row["fan_message"] != "":
            messages.append({ 
                "sender_type":"fan",
                "from":fan,
                "to":model,
                "message": row["fan_message"],
                "datetime": time,
                "revenue":row["revenue"]})
        
        if pd.notna(row["chatter_message"]) and row["chatter_message"] != "":
            messages.append({ 
                "sender_type":"model",
                "from":model,
                "to":fan,
                "message": row["chatter_message"],
                "datetime": chatter_time,
                "revenue": float('nan')})

    messages_df = pd.DataFrame(messages)
    messages_df = messages_df.sort_values(by="datetime")
    messages_df = messages_df.reset_index(drop=True)
    return messages_df

def condense_messages(messages:list):
    message_string = ""
    revenue = 0.0

    sender_type = messages[0]["sender_type"]
    sent_from = messages[0]["from"]
    sent_to = messages[0]["to"] 
    start_datetime = messages[0]["datetime"]
    end_datetime = messages[-1]["datetime"]

    for message in messages:
        message_string += (". " + message["message"])
        revenue += message["revenue"]
    
    return {
        "sender_type":sender_type,
        "revenue":revenue,
        "sent_from":sent_from,
        "sent_to":sent_to,
        "datetime":start_datetime,
        "message":message_string,
        "revenue":revenue
    }

def format_conversation_object(conversation_number:int, conversation:list):
    total_revenue = sum([convo["revenue"] for convo in conversation if not isnan(convo["revenue"])])

    first_conversation = conversation[0]
    last_conversation = conversation[-1]

    model_id = None
    fan_id = None

    if first_conversation["sender_type"] == "model":
        model_id = first_conversation["sent_from"]
        fan_id = first_conversation["sent_to"]
    else:
        fan_id = first_conversation["sent_from"]
        model_id = first_conversation["sent_to"]


    start_time = first_conversation["datetime"]
    last_time = last_conversation["datetime"]

    text = ""
    for convo in conversation:
        text += f"{convo['sent_from']}: {convo['message']} \n"

    return {
        "conversation_number":conversation_number,
        "fan_id":fan_id,
        "model_id":model_id,
        "total_revenue":total_revenue,
        "start_time":start_time,
        "last_time":last_time,
        "conversation_history":str(text),
    }

def chunk_conversations(messages_df, max_turns:int = 3, max_consecutive:int = 5, max_hours_gap:int = 5):
    MAX_TIME_GAP = timedelta(hours=max_hours_gap)

    conversation_sequence_counter = 0

    conversations = list()

    current_conversation = list()
    turns_in_this_conversation = 0

    messages_in_this_turn = list()
    sender_this_turn = None

    last_message_time = None

    for _, row in messages_df.iterrows():
        sender_type = row["sender_type"]
        sender_id = row["from"]
        time = row["datetime"]
        message = row["message"]

        if (last_message_time and (time - last_message_time > MAX_TIME_GAP)) or turns_in_this_conversation > max_turns:
            message_turn = condense_messages(messages_in_this_turn)
            current_conversation.append(message_turn)
            
            if current_conversation and turns_in_this_conversation > 1 and len(current_conversation) > 1:
                conversation_sequence_counter += 1
                conversations.append(format_conversation_object(conversation_sequence_counter, current_conversation))

            turns_in_this_conversation = 0
            current_conversation = list()
            conversational_turns = 1
            messages_in_this_turn = list()
            sender_this_turn = None

        if sender_id != sender_this_turn:
            if messages_in_this_turn:
                message_turn = condense_messages(messages_in_this_turn)
                current_conversation.append(message_turn)
            messages_in_this_turn = list()
            messages_in_this_turn.append(row.to_dict())
            turns_in_this_conversation += 1
        else:
            messages_in_this_turn.append(row.to_dict())
            pass

        
        if turns_in_this_conversation == 1 and len(messages_in_this_turn) > max_consecutive:
            messages_in_this_turn = messages_in_this_turn[1:]
        
        last_message_time = time
        sender_this_turn = sender_id
    
    if current_conversation and turns_in_this_conversation > 1 and len(current_conversation) > 1:
        conversation_sequence_counter += 1
        conversations.append(format_conversation_object(conversation_sequence_counter, current_conversation))


    return conversations
