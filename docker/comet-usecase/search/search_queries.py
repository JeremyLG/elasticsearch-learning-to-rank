import numpy as np


def multi_match_query(query, fields, size=10, fuzziness=None):
    """Returns the body of the multi_match query in Elasticsearch with possibility of setting
    the fuzziness on it"""
    query = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": fields
            }
        },
        "size": size
    }
    if fuzziness:
        query["query"]["multi_match"]["fuzziness"] = fuzziness
    return query


def autocomplete_query(query, fields, popularity_field, size=12):
    """Returns the body of the autocomplete query in Elasticsearch. On montre dans cet exemple que
    la fuzziness peut être basée sur des fonctions numpy par exemple.
    Can only be used with an autocomplete index"""
    return {
            "query": {
                "function_score": {
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": fields,
                            "fuzziness": np.sqrt(len(query)),
                            "analyzer": "autocomplete_search"
                        }
                    },
                    "field_value_factor": {
                        "field": popularity_field,
                        "modifier": "log1p",
                        "factor": 1
                    },
                    "boost_mode": "multiply",
                    "max_boost": 1
                }
            },
            "size": size
            }
