from pathlib import Path

class directories:
    ROOT = Path(__file__).resolve().parent
    DATA = ROOT / "data"

    RAW_DATA = DATA / "raw"
    FULL_DATASET = RAW_DATA / "full"
    MINI_DATASET = RAW_DATA / "mini"

    PROCESSED_DATA = DATA / "processed"
    COMBINED_DATA = PROCESSED_DATA / "formatted"
    INDIVIDUAL_DATA = PROCESSED_DATA / "individual"
    JSON_CONVERSATIONS = PROCESSED_DATA / "conversations_json"
    CONVERSATIONAL_DATA = PROCESSED_DATA / "conversations"
    EMBEDDED_CONVERSATIONS = PROCESSED_DATA / "embedded_conversations"
    JSON_EMBEDDED = PROCESSED_DATA / "embedded_conversations_json"

    RESOURCES = ROOT / "resources"
    LLM_PROMPTS = RESOURCES / "prompts"
    CHATBOT_SYSTEM_PROMPT = LLM_PROMPTS / "chatbot_assitance_system_context.txt"

    DIRS_TO_INIT = [DATA, FULL_DATASET, MINI_DATASET, COMBINED_DATA, INDIVIDUAL_DATA, JSON_CONVERSATIONS, CONVERSATIONAL_DATA, EMBEDDED_CONVERSATIONS, JSON_EMBEDDED]

class main_config:
    SKIP_ETL_PROCESS = True

    CUT_DOWN_CHATLOGS_SIZE = True
    # This only applies if the condition above is True
    MINIMUM_REVENUE_FROM_USER = 5000

    ENABLE_ELASTIC_PROCESS = True

    SAVE_DATA_TO_DISK = False

class etl_config:
    MANUAL_SUBSTITUTION_LIST = {
        "u":"you",
        "ur":"your",
        "beb":"babe",
        "rn":"right now",
        "bout":"about",
        "omg":"oh my god",
        "shiet":"shit",
        "dam":"damn"
    }

class chunk_config:
    MAX_UNREPLIED_MESSAGES = 5
    MAX_CONVERSATION_TURNS = 5 
    MAX_HOURS_BETWEEN_MESSAGES = 5

class elastic_config:
    CLOUD_URL = "https://efd270af53f34c8aa2dc05f200036552.asia-southeast1.gcp.elastic-cloud.com:443"
    BULK_UPLOAD_TIMEOUT = 60
    QUERY_TIMEOUT = 60
    CHATLOGS_INDEX_NAME = "fandom-chatlogs"
    CONVERSATIONS_INDEX_NAME = "fandom-conversations"

class llm_config:
    GROK_URL="https://api.x.ai/v1/chat/completions"
