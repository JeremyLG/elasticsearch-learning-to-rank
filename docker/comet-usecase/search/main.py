import yaml
import logging
import logging.config
import logging.handlers
import pathlib

import processing as P
import search as S
import nlp as N
from config import CONF_DIR, INDEX_READ, RUN_MODES, LOG_FILE_PATH, PROCESSED_INDEX, FORCE_REWRITE
from utils import index_exists_and_has_data


def initializes_logger():
    """As function name says"""
    logging.config.dictConfig(yaml.safe_load(open(CONF_DIR + 'logging.yml', 'r')))


def run_modes():
    """Run modes instrumented in config file"""
    for mode in RUN_MODES:
        if mode == "check_index":
            logging.info("Checking that TMDB Elasticsearch index is created")
            if index_exists_and_has_data(INDEX_READ):
                logging.info("Elasticsearch index has been created and indexed with data")
            else:
                logging.error("Elasticsearch index isn't created or hasn't been indexed with data")
                raise ValueError("Innapropriate value for count of data in Elasticsearch index")
        if mode == "processing":
            if not(index_exists_and_has_data(PROCESSED_INDEX)) or FORCE_REWRITE:
                logging.info("Starting processing the index")
                P.main()
        if mode == "searching":
            logging.info("Starting to search in the index processed")
            S.main()
        if mode == "nlp":
            logging.info("Starting the NLP pipeline before indexing to ES")
            N.main()


def main():
    """Check logs directory exists, open log file for logger, initializes the logger and run"""
    check_directories_exists()
    open(LOG_FILE_PATH, 'w').close()
    initializes_logger()
    run_modes()


def check_directories_exists():
    """Checking logs and models directories"""
    pathlib.Path('logs/').mkdir(parents=True, exist_ok=True)
    pathlib.Path('models/').mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    main()
