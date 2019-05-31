import os
import json
import elasticsearch
import requests
from requests.auth import HTTPBasicAuth

__all__ = ["ES_AUTH", "ES_HOST", "ES_INDEX", "ES_TYPE", "ES_DATA",
           "ES_FEATURE_SET_NAME", "ES_MODEL_TYPE", "ES_METRIC_TYPE",
           "ES_OBJECT"]

ES_HOST = os.environ['ES_HOST']
ES_USER = os.environ.get('ES_USER', None)
ES_PASSWORD = os.environ.get('ES_PASSWORD', None)
ES_INDEX = os.environ.get('ES_INDEX', 'tmdb')
ES_TYPE = os.environ.get('ES_TYPE', 'movie')
ES_DATA = os.environ.get('ES_DATA', '/opt/services/flaskapp/tmdb.json')
ES_FEATURE_SET_NAME = os.environ.get('ES_FEATURE_SET_NAME', 'movie_features')
ES_MODEL_NAME = os.environ.get('ES_MODEL_NAME', 'test_6')
ES_MODEL_TYPE = os.environ.get('ES_MODEL_TYPE', '6')
ES_METRIC_TYPE = os.environ.get('ES_METRIC_TYPE', 'ERR@10')

if ES_USER is not None and ES_PASSWORD is not None:
    auth = (ES_USER, ES_PASSWORD)
    ES_AUTH = HTTPBasicAuth(*auth)
else:
    auth = None
    ES_AUTH = None

ES_OBJECT = elasticsearch.Elasticsearch(ES_HOST, timeout=1000, http_auth=auth)


def index_exists_and_has_data(index):
    """Check si l'index de base a été crée et s'il contient bien de la donnée indexée"""
    if ES_OBJECT.indices.exists(index=index):
        count_all_query = {
            "query": {
                "match_all": {}
            }
        }
        count = ES_OBJECT.count(index=index, body=count_all_query)
        return (count and count != 0)
    else:
        return False


def insertDataframeIntoElastic(df, fields, index=ES_INDEX, typ=ES_TYPE,
                               server=ES_HOST, chunk_size=2000):
    """Currently not used. Doesn't work well, bulk with a generator is much better.
    But still not perfect. TO IMPROVE"""
    headers = {'content-type': 'application/x-ndjson',
               'Accept-Charset': 'UTF-8'}
    records = df[fields].to_dict(orient='records')
    actions = ["""{ "index" : { "_index" : "%s", "_type" : "%s"} }\n""" %
               (index, typ) + json.dumps(records[j]) for j in range(len(records))]
    i = 0
    while i < len(actions):
        serverAPI = server + '/_bulk'
        data = '\n'.join(actions[i:min([i+chunk_size, len(actions)])])
        data = data + '\n'
        requests.post(serverAPI, data=data, headers=headers)
        print(i)
        i = i+chunk_size
