from collections import ChainMap


def autocomplete_settings():
    """Index settings for an autocomplete search"""
    return {
      "settings": {
        "analysis": {
          "analyzer": {
            "autocomplete": {
              "tokenizer": "autocomplete",
              "filter": ["lowercase"]
            },
            "autocomplete_search": {
              "tokenizer": "lowercase"
            }
          },
          "tokenizer": {
            "autocomplete": {
              "type": "edge_ngram",
              "min_gram": 2,
              "max_gram": 20,
              "token_chars": ["letter"]
            }
          }
        }
      }
    }


def autocomplete_field(field):
    """Create a dictionnary for a field in the index mappings for the
    autocomplete feature in the search engine"""
    return {
        field: {
          "type": "text",
          "fielddata": True,
          "analyzer": "autocomplete",
          "search_analyzer": "autocomplete_search"
          }
        }


def one_line_dict_merging(fields):
    """Construct a list of dicts for each field in the dataframe that should
    be used in the autocomplete search engine.
    Then with the ChainMap object, it merges the list into one dict."""
    return dict(ChainMap(*[autocomplete_field(field) for field in fields]))


def autocomplete_mappings(fields, ES_TYPE):
    """Creating the final mappings of the index for each of the field to
    index"""
    return {
        "mappings": {
            ES_TYPE: {
                "properties": one_line_dict_merging(fields)
                }
            }
        }
