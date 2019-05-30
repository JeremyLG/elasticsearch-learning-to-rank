from collections import ChainMap
from abc import ABC, abstractmethod


class IndexCreation(ABC):

    def __init__(self, fields, es_type):
        self.fields = fields
        self.es_type = es_type
        self.index_settings_mappings = {}

    @abstractmethod
    def _settings(self):
        pass

    @abstractmethod
    def _generate_field_dict(self, field):
        pass

    def _one_line_dict_merging(self):
        """Construct a list of dicts for each field in the dataframe that should
        be used in the autocomplete search engine.
        Then with the ChainMap object, it merges the list into one dict."""
        return dict(ChainMap(*[self._generate_field_dict(field) for field in self.fields]))

    def _mappings(self):
        """Creating the final mappings of the index for each of the field to
        index"""
        return {
          "mappings": {
            self.es_type: {
                "properties": self._one_line_dict_merging()
                }
            }
        }

    def _create_index(self):
        """Set les settings et mappings de l'index"""
        self.index_settings_mappings = {
            **self._settings(),
            **self._mappings()
            }

    def put_index(self, es, index_name):
        """Putting index with mappings and settings in Elasticsearch"""
        self._create_index()
        es.indices.delete(index_name, ignore=[400, 404])
        es.indices.create(index_name, body=self.index_settings_mappings)


class AutocompleteIndex(IndexCreation):
    def _settings(self):
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

    def _generate_field_dict(self, field):
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


class FrenchAnalyserIndex(IndexCreation):
    def _settings(self):
        return {
          "settings": {
            "analysis": {
              "filter": {
                "french_elision": {
                  "type": "elision",
                  "articles_case": True,
                  "articles": ["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu",
                               "lorsqu", "puisqu"]
                },
                # "french_synonym": {
                #   "type": "synonym",
                #   "ignore_case": True,
                #   "expand": True,
                #   # "synonyms": [
                #   #   POSSIBILITé DE DéFINIR DES SYNONYMES ICI OU DE LINK UN FICHIER
                #   # ]
                # },
                "french_stemmer": {
                  "type": "stemmer",
                  "language": "light_french"
                }
              },
              "analyzer": {
                "french_heavy": {
                  "tokenizer": "icu_tokenizer",
                  "filter": [
                    "french_elision",
                    "icu_folding",
                    # "french_synonym",
                    "french_stemmer"
                  ]
                },
                "french_light": {
                  "tokenizer": "icu_tokenizer",
                  "filter": [
                    "french_elision",
                    "icu_folding"
                  ]
                }
              }
            }
          }
        }

    def _generate_field_dict(self, field):
        """Create a dictionnary for a field in the index mappings for the
        basic indexation feature in the search engine according to french"""
        return {
            field: {
                "type": "text",
                "analyzer": "french_light",
                "fields": {
                  "stemmed": {
                    "type": "text",
                    "analyzer": "french_heavy"
                  }
                }
              }
            }


class BasicEnglishIndex(IndexCreation):
    def _settings(self):
        return {
            "settings": {
                "analysis": {
                  "filter": {
                    "english_stop": {
                      "type": "stop",
                      "stopwords": "_english_"
                    },
                    "english_keywords": {
                      "type": "keyword_marker",
                      "keywords": ["conventional", "hosting"]
                    },
                    "english_stemmer": {
                      "type": "stemmer",
                      "Language": "english"
                    },
                    "english_possessive_stemmer": {
                      "type": "stemmer",
                      "Language": "possessive_english"
                    }
                  },
                  "analyzer": {
                    "english": {
                      "tokenizer": "standard",  # possibilité d'utiliser l'icu_tokenizer
                      "filter": [
                        "english_possessive_stemmer",
                        "lowercase",
                        "english_stop",
                        "english_keywords",
                        "english_stemmer"
                      ]
                    }
                  }
                }
            }
        }

    def _generate_field_dict(self, field):
        """Create a dictionnary for a field in the index mappings for the
        basic indexation feature in the search engine according to french"""
        return {
            field: {
                "type": "text",
                "analyzer": "english"
              }
            }
