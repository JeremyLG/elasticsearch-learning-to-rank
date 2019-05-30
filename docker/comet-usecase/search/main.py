import yaml
import logging
import logging.config
import logging.handlers
import pathlib
from utils import index_exists_and_has_data
import preprocessing as P

CONF_DIR = "conf/"
CONF_FILE = "prod.yml"


def main(conf_dict):
    logging.config.dictConfig(yaml.safe_load(open(CONF_DIR + 'logging.yml', 'r')))
    for mode in conf_dict["run"]["modes"].split(","):
        if mode == "check_index":
            logging.info("Checking that TMDB Elasticsearch index is created")
            if index_exists_and_has_data():
                logging.info("Elasticsearch index has been created and indexed with data")
            else:
                logging.error("Elasticsearch index isn't created or hasn't been indexed with data")
                raise ValueError("Innapropriate value for count of data in Elasticsearch index")
        if mode == "processing":
            logging.info("Starting processing the index")
            P.main(conf_dict["run"]["mode_options"]["processing"], conf_dict["run"]["level"])
        if mode == "nlp":
            logging.info("Starting the NLP pipeline before indexing to ES")
            pass


def check_logs_directory_exists():
    pathlib.Path('logs/').mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    conf_dict = yaml.safe_load(open(CONF_DIR + CONF_FILE, 'r'))
    check_logs_directory_exists()
    log_file_path = conf_dict["directories"]["logging_path"]
    open(log_file_path, 'w').close()
    main(conf_dict)
