import yaml

CONF_DIR = "conf/"
CONF_FILE = "prod.yml"
conf_dict = yaml.safe_load(open(CONF_DIR + CONF_FILE, 'r'))

RUN_MODES = conf_dict["run"]["modes"].split(",")
LOG_LEVEL = conf_dict["run"]["level"]
LOG_FILE_PATH = conf_dict["directories"]["logging_path"]

processing_dict = conf_dict["run"]["mode_options"]["processing"]
INDEXATION_MODE = processing_dict["indexation_mode"]
INDEX_READ = processing_dict["index"]
PROCESSED_INDEX = INDEX_READ + "_" + INDEXATION_MODE
FORCE_REWRITE = processing_dict["force_rewrite"]
FIELDS_TO_INDEX = processing_dict["fields_to_index"].split(",")
FIELDS_TO_KEEP = processing_dict["supplementary_fields"].split(",") + FIELDS_TO_INDEX
ID_FIELD = processing_dict["id_field"]

searching_dict = conf_dict["run"]["mode_options"]["searching"]
SEARCHING_INDEX = searching_dict["index"]
QUERY = searching_dict["query"]
POPULARITY_FIELD = searching_dict["popularity_field"]
FUZZINESS = searching_dict["fuzziness"]
FIELDS_TO_SEARCH = searching_dict["fields_to_search"]

nlp_dict = conf_dict["run"]["mode_options"]["nlp"]
FIELD_TO_EMBED = nlp_dict["field_to_embed"]
RETRAIN_MODEL = nlp_dict["retrain_model"]
MODEL_PATH = nlp_dict["model_path"]
