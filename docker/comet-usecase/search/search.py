from elasticsearch_dsl import Search
import logging
import json

from search_queries import multi_match_query, autocomplete_query
from config import (SEARCHING_INDEX, FIELDS_TO_SEARCH, INDEXATION_MODE, QUERY, FUZZINESS,
                    POPULARITY_FIELD, LOG_LEVEL)
from utils import ES_OBJECT

logger = logging.getLogger(__name__)


def execute_and_log_overview(s):
    """Execute and log the search query"""
    logging.info("Executing the basic multi match query")
    s.execute()
    logging.info(s)
    if LOG_LEVEL == "DEBUG":
        logging.info("Search query qui a été appliqué")
        logging.info(json.dumps(s.to_dict(), indent=2))
        logging.info("Printing des top hits")
        for hit in s:
            try:
                logging.info(f"Titre du film trouvé : {hit.title}")
            except KeyError:
                logging.warning("There is currently not the title key indexed")
                logging.info(f"Synopsis du film trouvé : {hit.overview}")
    logging.info("Search finished executing in ")


def apply_query():
    """Apply the correct query adapted to the config file and how the data has been indexed"""
    s = Search(using=ES_OBJECT, index=SEARCHING_INDEX)
    if INDEXATION_MODE == "autocomplete":
        logging.info("Applying autocomplete search")
        s.update_from_dict(
            autocomplete_query(QUERY, FIELDS_TO_SEARCH, popularity_field=POPULARITY_FIELD)
        )
    elif INDEXATION_MODE in ["basic_english", "french"]:
        logging.info("Applying multi match search with fuzziness if set in yaml")
        s.update_from_dict(
            multi_match_query(QUERY, FIELDS_TO_SEARCH, fuzziness=FUZZINESS)
        )
    else:
        raise NotImplementedError("Mode d'indexation choisi pas setup")
    return s


def main():
    s = apply_query()
    execute_and_log_overview(s)


if __name__ == '__main__':
    main()
