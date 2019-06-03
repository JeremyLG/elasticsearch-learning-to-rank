import pandas as pd
import time
import elasticsearch
import json
import logging
from multiprocessing import Pool
from elasticsearch_dsl import Search
# from collections import deque

from index_creation import AutocompleteIndex, FrenchAnalyserIndex, BasicEnglishIndex
from utils import ES_OBJECT, ES_INDEX, ES_TYPE  # , insertDataframeIntoElastic
from config import (PROCESSED_INDEX, FIELDS_TO_INDEX, FIELDS_TO_KEEP, INDEXATION_MODE, LOG_LEVEL,
                    ID_FIELD)

logger = logging.getLogger(__name__)
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

count_all_query = {
    "query": {
        "match_all": {}
    }
}
SLICES = 5
clean_columns = ["budget", "revenue", "title", "original_language",
                 "popularity", "overview", "release_date",
                 "tagline", "vote_average", "vote_count", "id"]
dirty_columns = ["cast", "directors", "genres"]


def elastic_thread_dataframe_reader(slice_no):
    """Par thread, lis les JSON via l'API Slice Scrolling d'Elasticsearch puis
    les transforme en dictionnaire pour l'ajouter à un dataframe pandas"""
    s = Search(using=ES_OBJECT, index=ES_INDEX)
    s = s.extra(slice={"id": slice_no, "max": SLICES})
    return pd.DataFrame((d.to_dict() for d in s.scan()))


def multithread_dataframe_concat():
    """Lecture multi-threadée des données et concat en un unique dataframe.
    Retry X times parce que cela peut crash sur une random decoding error"""
    logging.info("Creating pool of threads")
    pool = Pool(SLICES)
    while True:
        try:
            logging.info("Trying to read via multi-threading")
            results = pool.map(elastic_thread_dataframe_reader, range(SLICES))
            df = pd.concat(results)
        except (json.decoder.JSONDecodeError, elasticsearch.TransportError):
            logging.error("Error de decoding ou transport Elasticsearch")
            logging.info("Waiting 5 secs")
            time.sleep(5)
            continue
        break
    logging.info("Closing workers pool")
    pool.close()
    pool.join()
    logging.info("Returning dataframe pandas")
    return df


def elasticsearch_easy_read_pandas(index=ES_INDEX):
    """Le multi-threading n'est pas parfaitement fault tolerant à cause d'un faible nombre de
    thread en local sur le PC. Plus également perdu dans l'ouverture des sockets.
    Donc implémentation d'un easy_read pour un effet démo plus sûr"""
    s = Search(using=ES_OBJECT, index=index)
    count = sum(1 for _ in s.scan())
    s = s.params(preserve_order=True)
    # output_all = deque()
    # # Extend deque with iterator
    # output_all.extend(d.to_dict() for d in s.scan())
    # # Convert deque to DataFrame
    # output_df = pd.io.json.json_normalize(output_all)
    df = pd.DataFrame((d.to_dict() for d in s.scan()))
    assert df.shape[0] == count
    return df


def drop_nan_rows(df, columns):
    """Fonction qui delete les valeurs nulles sur les colonnes intéressantes"""
    return (df
            .dropna(subset=columns)
            .reset_index(drop=True))


def flatten_column_array(df, columns, separator="|"):
    """Fonction qui transforme une colonne de strings séparés par un
    séparateur en une liste : String column -> List column"""
    df[columns] = (
        df[columns].applymap(lambda x: separator.join(
            [str(json_nested["name"]) for json_nested in x]))
        )
    return df


def safe_value(df, column):
    """Currently only used for popularity"""
    df[column] = df[column].fillna(0)
    return df


def base_url_poster():
    """Currently not used"""
    # image size : original ou w185 par exemple
    image_size = "w185"
    base_url = "http://image.tmdb.org/t/p/"
    return base_url + image_size


def filterKeys(document):
    """Filtering keys to index in the document generator"""
    return {key: document[key] for key in FIELDS_TO_KEEP}


def doc_generator(df):
    """Elasticsearch document generator from pandas dataframe to be bulked"""
    df_iter = df.iterrows()
    for index, document in df_iter:
        try:
            yield {
                "_index": PROCESSED_INDEX,
                "_type": ES_TYPE,
                "_id": f"{document[ID_FIELD]}",
                "_source": filterKeys(document),
            }
        except StopIteration:
            return


def index_mode(mode):
    if mode == "autocomplete":
        return AutocompleteIndex(FIELDS_TO_INDEX, ES_TYPE)
    elif mode == "french":
        return FrenchAnalyserIndex(FIELDS_TO_INDEX, ES_TYPE)
    elif mode == "basic_english":
        return BasicEnglishIndex(FIELDS_TO_INDEX, ES_TYPE)
    else:
        raise NotImplementedError("The mode chose has not been implemented yet!")


def main():
    df = elasticsearch_easy_read_pandas()
    processed_df = (
        df
        .pipe(drop_nan_rows, columns=dirty_columns+clean_columns)
        .pipe(flatten_column_array, columns=dirty_columns)
        .pipe(safe_value, column="popularity")
        )
    index = index_mode(INDEXATION_MODE)
    index.put_index(ES_OBJECT, PROCESSED_INDEX)
    elasticsearch.helpers.bulk(ES_OBJECT, doc_generator(processed_df))
    # Doesn't index much more than 10 000...
    # insertDataframeIntoElastic(processed_df, FIELDS_TO_INDEX, PROCESSED_INDEX,
    #                            chunk_size=10000)
    # print(es.count(index=PROCESSED_INDEX, body=count_all_query))
    # print(processed_df.shape)
    # create_index(es)
    if LOG_LEVEL == "DEBUG":
        count_es = ES_OBJECT.count(index=PROCESSED_INDEX, body=count_all_query)["count"]
        logging.info(f"Nombre de documents indexés dans Elasticsearch {count_es}")
        logging.info(f"Shape du dataframe indexé {processed_df.shape}")
        logging.warning(f"There is a difference of {processed_df.shape[0] - count_es} "
                        f"documents indexed in Elasticsearch from the pandas dataframe"
                        f" from initial {processed_df.shape[0]} documents in dataframe")
        # Perte d'une centaine de films à cause de champs vides most likely
        # C'est toujours mieux que la première méthode dans tous les cas


if __name__ == '__main__':
    main()
