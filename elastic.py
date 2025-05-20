from elasticsearch import Elasticsearch, helpers
import pandas as pd
from config import elastic_config
import logging
from numbers import Number
from elasticsearch.helpers import BulkIndexError
logger = logging.getLogger(__name__)

def get_elatic_client(cloud_url:str, api_key:str):
    return Elasticsearch(
    cloud_url,
    api_key = api_key,
)

def upload_documents(documents:list[dict], client:Elasticsearch, index_name:str, timeout:int = 60):
    try:
        success_response, fail_response = helpers.bulk(client, documents, index = index_name, request_timeout = timeout)
    except BulkIndexError as e:
        for error in e.errors:
            index_error = error.get('index', {})
            print("Reason:", index_error.get('error', {}).get('reason'))
            print("Type:  ", index_error.get('error', {}).get('type'))
            print("Caused by:  ", index_error.get('error', {}).get('caused_by'))
            print("Doc:   ", index_error.get('data'))
            print("-" * 40)
            
        # logger.info(f"{success_response}")

        # if fail_respose:
        #     logger.error(f"{fail_response}")

def get_match_query(field:str, text:str) -> dict:
    return {
        "match": {
            field: text
        }
    }

def get_semantic_query(field:str, text:str) -> dict:
    return {
        "semantic": {
            "field":field,
            "query":text
        }
    }

def get_term_query(field:str, value:str) -> dict:
    return {
        "term": {
            field: {
                "value": value
            }
        }
    }

def get_numeric_range_query(field:str, gte:Number = None, lte:Number = None) -> dict:
    if not ((gte is None) or (lte is None)):
        if type(gte) is not type(lte):
            raise TypeError(f"Type mismatch: {type(gte).__name__} != {type(lte).__name__}")

    range_body = dict()

    if gte is not None:
        range_body["gte"] = gte
    if lte is not None:
        range_body["lte"] = lte
    return {
        "range": {
            field: range_body
        }
    }

def get_bool_query(must_query:list[dict] = None, should_query:list[dict] = None, filter_query:list[dict] = None, must_not_query:list[dict] = None):
    query = {"bool": {}}

    if must_query:
        query["bool"]["must"] = must_query
    if should_query:
        query["bool"]["should"] = should_query
    if filter_query:
        query["bool"]["filter"] = filter_query
    if must_not_query:
        query["bool"]["must_not"] = must_not_query

    return query

def run_query(query_body:dict, client:Elasticsearch, index_name:str):
    logger.info(f"Sending query to elasticsearch at {index_name}")
    response = client.search(index = index_name, query = query_body, timeout = f"{elastic_config.QUERY_TIMEOUT}s")
    logger.debug(f"Elasticsearch Response:\n{response}")

    context = [hits["_source"] for hits in response["hits"]["hits"]]
    return context


