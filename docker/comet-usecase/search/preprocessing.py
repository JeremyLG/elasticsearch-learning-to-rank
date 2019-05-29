from multiprocessing import Pool
from elasticsearch_dsl import Search
import pandas as pd
from utils import Elasticsearch, ES_INDEX, ES_TYPE  # , insertDataframeIntoElastic
import time
import elasticsearch
import json

es = Elasticsearch(timeout=15)
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
PROCESSED_INDEX = "tmdb_preprocessed"
FIELDS_TO_KEEP = ["id", "overview", "cast", "genres"]
FIELDS_TO_INDEX = ["overview", "cast", "genres"]


def elastic_thread_dataframe_reader(slice_no):
    """Par thread, lis les JSON via l'API Slice Scrolling d'Elasticsearch puis
    les transforme en dictionnaire pour l'ajouter à un dataframe pandas"""
    s = Search(using=es, index=ES_INDEX)
    s = s.extra(slice={"id": slice_no, "max": SLICES})
    return pd.DataFrame((d.to_dict() for d in s.scan()))


def multithread_dataframe_concat():
    """Lecture multi-threadée des données et concat en un unique dataframe.
    Retry X times parce que cela peut crash sur une random decoding error"""
    pool = Pool(SLICES)
    while True:
        try:
            results = pool.map(elastic_thread_dataframe_reader, range(SLICES))
            df = pd.concat(results)
        except (json.decoder.JSONDecodeError, elasticsearch.TransportError):
            time.sleep(5)
            continue
        break
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


def safe_value(field_val):
    """Currently not used"""
    return field_val if not pd.isna(field_val) else "Other"


def base_url_poster():
    """Currently not used"""
    # image size : original ou w185 par exemple
    image_size = "w185"
    base_url = "http://image.tmdb.org/t/p/"
    return base_url + image_size


def create_index(es, index=PROCESSED_INDEX, es_type=ES_TYPE):
    """Creating index with mappings and settings"""
    from build_mappings import autocomplete_settings, autocomplete_mappings
    settings = {**autocomplete_settings(),
                **autocomplete_mappings(FIELDS_TO_INDEX, es_type)}

    es.indices.delete(index, ignore=[400, 404])
    es.indices.create(index, body=settings)
    # elasticsearch.helpers.bulk(es, self.__bulkDocs(movieDict, index, es_type))


def filterKeys(document):
    """Filtering keys to index in the document generator"""
    return {key: document[key] for key in FIELDS_TO_INDEX}


def doc_generator(df):
    """Elasticsearch document generator from pandas dataframe to be bulked"""
    df_iter = df.iterrows()
    for index, document in df_iter:
        try:
            yield {
                "_index": PROCESSED_INDEX,
                "_type": ES_TYPE,
                "_id": f"{document['id']}",
                "_source": filterKeys(document),
            }
        except StopIteration:
            return


def main():
    df = multithread_dataframe_concat()
    processed_df = (
        df
        .pipe(drop_nan_rows, columns=dirty_columns+clean_columns)
        .pipe(flatten_column_array, columns=dirty_columns)
        )
    create_index(es)
    # Doesn't index much more than 10 000...
    # insertDataframeIntoElastic(processed_df, FIELDS_TO_INDEX, PROCESSED_INDEX,
    #                            chunk_size=10000)
    # print(es.count(index=PROCESSED_INDEX, body=count_all_query))
    # print(processed_df.shape)
    # create_index(es)
    elasticsearch.helpers.bulk(es,
                               doc_generator(processed_df[FIELDS_TO_KEEP]))
    print(es.count(index=PROCESSED_INDEX, body=count_all_query))
    print(processed_df.shape)
    # Perte d'une centaine de films à cause de champs vides most likely
    # C'est toujours mieux que la première méthode dans tous les cas


if __name__ == '__main__':
    main()
