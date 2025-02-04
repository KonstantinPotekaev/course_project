import os
import argparse
import hashlib
import json
import pickle
from base64 import b64encode, b64decode
from collections import namedtuple
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Iterable, Generator, Optional, Collection, Any, Union

import requests
import yaml

from utils.date import DATE_FORMAT

import utils.exceptions as ut_exc

##############################
# Параметры задач и запросов #
##############################

# поля ответа requests
CODE = "code"
MESSAGE = "message"

MODULE = "module"
TARGET_MODULES = "target_modules"
USER_ID = "user_id"
USER_IDS = "user_ids"
DATE_TIME = "date_time"
DATE_TIME_UTC = "date_time_utc"
FACT_FULL_ID = "fact_full_id"
FACT_ID = "fact_id"
FACT_IDS = "fact_ids"
SOURCE = "source"
CONTACT = "contact"
CONTACT_ID = "contact_id"
CONTACT_TYPE = "contact_type"
INTERNAL = "internal"
INTERVAL = "interval"
INTERVAL_1D = "1d"
TASK_TYPE = "task_type"
ADDRESSEES = "addressees"
TO = "to"
UPDATED = "updated"
APP_ID = "app_id"
APP_NAME = "app_name"
APP_DESCRIPTION = "app_description"
APP_EXECUTABLE_NAME = "app_executable_name"
APP_PATH = "app_path"
APP_SITE = "app_site"
APP_SITE_URI = "app_site_uri"
APP_WINDOW_NAME = "app_window_name"
USERS_RATIO = "users_ratio"
USERS_NUMBER = "users_number"
APPS = "apps"
USERS_APPS = "users_apps"
NEW_APPS_NUMBER = "new_apps_number"

COND = "cond"
WIN = "win"
STATUS = "status"

ITEMS = "items"
COUNT = "count"

# Количество последних дней которые будут отображены в интерфейсе, если не заданы даты
DEFAULT_LAST_DAYS_IN_INTERFACE = 7

START_DATE = "start_date"
END_DATE = "end_date"
CUR_DATE = "cur_date"
START_DATE_TIME_INCLUSIVE = "start_date_time_inclusive"
END_DATE_TIME_NON_INCLUSIVE = "end_date_time_non_inclusive"
INSERT_DATE = "insert_date"
FACT_TUPLE_IDS = "fact_tuple_ids"
STAT_NAME = "stat_name"
STAT_NAMES = "stat_names"
DATES = "dates"
VALUE = "value"
MIN_VALUE = "min_value"
MAX_VALUE = "max_value"
SORT_BY = "sort_by"
SORT_BY_SCORE = "user_score"
SORT_BY_DIFF = "diff"
SORT_ORDER = "sort_order"
GROUP_BY_DATE = "group_by_date"
GROUP_BY_DATE_PERIOD = "group_by_date_period"
ASC = "asc"
DESC = "desc"
SELECT_FIELDS = "select_fields"
WHERE = "where"
FROM = "from"
IN = "in"
NOT = "not"
SERVICE_NAME = "service_name"
YEAR = "year"
WEEK = "week"
CREATED_AT = "created_at"
DUMPED_AT = "dumped_at"
DUMP_FILE = "dump_file"
PARENT_TABLE = "parent_table"

ID = "id"
DEPENDENCIES = "dependencies"
DEPENDENCY = "dependency"
NOTIFICATION_PREFIX = "notif"
PREFIX = "prefix"
USERS = "users"
USER = "user"
WEIGHT = "weight"
SCORE = "score"
TOTAL_COUNT = "total_count"
TOTAL_SCORE = "total_score"
VARIABLE_PART = "variable_part"
IS_TOTAL = "is_total"
RECIPIENTS = "recipients"
FREQUENCY = "frequency"
MINIMAL_RATING_CHANGE = "minimal_rating_change"
MINIMAL_RATING = "minimal_rating"
MINIMAL_ANOMALY = "minimal_anomaly"
SCORE_VALUE = "score_value"
ANOMALY_VALUE = "anomaly_value"
NOTIFICATION_ID = "notification_id"
KIND = "kind"
LOCALIZATION = "localization"
PRIORITY = "priority"
CHILDREN = "children"
MODELS = "models"
MODEL = "model"
MODEL_ID = "model_id"
NAME = "name"
ORDER_BY = "order_by"
SUM_SCORE = "sum_score"
FIRE_DATE = "fire_date"
EMAILS = "emails"
CONTACTS = "contacts"
COLOR = "color"
ICON = "icon"
USERS_CONTACTS = "users_contacts"
PERSONAL_EMAILS = "personal_emails"
INTERNAL_CONTACT_BITMAP = "internal_contact_bitmap"
EMAIL_CONTACT_BITMAP = "email_bitmap"
UNDER_CONTROL = "under_control"
RELATED_EVENT_ID = "related_event_id"
STATISTICS_WITH_RELATED_EVENTS = "statistics_with_related_events"
FREE_MAIL_SERVICES_ALL = "free_mail_services_all"
IN_ORDER = "in_order"
DATA = "data"
PREDICTION_LICENSE = "Prediction"
STAT_PROBABILITY_PARAMS = "stat_probability_params"
CALENDAR_STAT_PROBABILITY_PARAMS = "calendar_stat_probability_params"
APPLICATIONS_ACTIVITIES_PARAMS = "applications_activities_params"
SERVICES = "services"
SERVICES_CONFIGURATION = "services_configuration"
SPECIFIC_PARAMS = "specific_params"
GENERAL_PARAMS = "general_params"
DESCRIPTION = "description"
AUTO_ADDRESSES = "auto_addressees"
STATISTICS_WITH_EVENTS = "statistics_with_events"
HANDLER = "handler"
PARAMS = "params"
TREND_LOOKUP_PERIOD_DAYS = "trend_lookup_period_days"
MIN_NON_ZERO_DAYS_RATIO = "min_non_zero_days_ratio"
MIN_NON_ZERO_PERIOD_DURATION_RATIO = "min_non_zero_period_duration_ratio"
MAX_DELTA_T_RATIO = "max_delta_t_ratio"
MIN_TREND_QUALITY = "min_trend_quality"
N_SIGMA = "n_sigma"
TRENDS = "trends"
DEPS_CHAIN = "deps_chain"
BASE_STATS = "base_stats"

METRICS_IN_THE_EMAIL = "metrics_in_the_email"
SEND_EMAIL = "send_email"
COMMUNICATION_CHANNELS = "communication_channels"
SERVER = "server"
STATISTIC_NAME = "statistic_name"
LOGIN = "login"
PASSWORD = "password"
FROM_EMAIL = "from_email"
TO_EMAIL = "to_email"
SUBJECT = "subject"
HTML_PATH = "html_path"
PORT = "port"
SUP_SSL = "sup_ssl"
NOTIFICATIONS = "notifications"
MIN_VAL = "min_val"
MAX_VAL = "max_val"
TEMPLATE = "template"
DESCR_PATH = "descr_path"
EMAIL_TEMPLATE = "email_template"
SEND_EMAIL_COMPUTER = "send_email_computer"
LEN_CHUNK = "len_chunk"
MINUTES = "minutes"
STAT_DESCRIPTION = "stat_description"
STAT_METRIC = "stat_metric"
STATISTIC_VALUE = "statistic_value"
CONDITIONS = "conditions"
NOTICE_SENDER = "notice_sender"
CHANNELS = "channels"
TYPE = "type"
TEMPLATE_ID = "template_id"
DOSSIER_REPLICA_PROVIDER = "dossier_replica_provider"
CHUNK_SIZE = "chunk_size"
CHUNK = "chunk"
METRICS = "metrics"
TIME = "time"
CONFIGURATION_PROVIDER = "configuration_provider"
IS_SENT = "is_sent"

CREATE_DATE_TIME_TS = "create_date_time_ts"
MODIFY_DATE_TIME_TS = "modify_date_time_ts"
MODIFICATION_DATE_TIMES_TS = "modification_date_times_ts"
INTEGRATION_ID = "integration_id"
SOURCE_ID = "source_id"
SOURCE_NAME = "source_name"
SOURCES_IDS = "sources_ids"
ROUTE = "route"
VIEW = "view"
VIEWS = "views"
POLICY_INFO = "policy_info"
LAST_UPDATE_DT = "last_update_dt"
NON_WORK_DAYS = "non_work_days"
DAY_STATUS = "day_status"
HOST = "host"
CREDENTIALS = "Credentials"
REQUEST_TBS_ID = "tbsId"
REQUEST_CONTENT_ID = "contentId"
QUERIES_ACCESS_ID = "queries_access_id"

LAST_CHUNK = "last_chunk"
FIRST_CHUNK = "first_chunk"
CHUNK_NUMBER = "chunk_number"
DOSSIER_REPLICA = "dossier_replica"
DOSSIER_REPLICA_HASH = "dossier_replica_hash"
DOSSIERS_DATA = "dossiers_data"

USER_NAME = "user_name"
DEPARTMENT = "department"
DEPARTMENT_IDS = "department_ids"
AD_GROUP = "ad_group"
AD_GROUPS = "ad_groups"
POSITION = "position"
DISABLED_DATE = "disabled_date"
ENABLED = "enabled"
MANAGER_ID = "manager_id"
FILIAL = "filial"

POLICIES = "policies"
PROTECTED_OBJECTS = "protected_objects"
DAYS = "days"

DEPARTMENTS = "departments"

STATS_INFO = "statistics_info"
STAT_VALUE = "stat_value"
STAT_INFLUENCE = "stat_influence"
STAT_DESCRIPTION_TEMPLATE = "stat_descr_template"
STAT_DESCRIPTION_TEMPLATE_DICT = "stat_descr_templates"

PROFILE_DEPTH = "profile_depth"
PROFILE_BIAS = "profile_bias"
PROFILE_HALF_LIFE = "profile_half_life"

ALLOW_NULL = "allow_null"

FREE_MAIL_SERVICES = "free_mail_services"

APPLICATIONS = "applications"
STATISTICS = "statistics"
IW_ACCESS_TOKEN = "iwAccessToken"
AUTHORIZATION = "Authorization"

ROOT_SESSION_ID = "root_session_id"
SESSION_PATH = "session_path"
PARENT_SESSION_PATH = "parent_session_path"
SUBCLUSTERS = "subclusters"
PARENT_CLUSTER_ID = "parent_cluster_id"
CLUSTER_COUNT = "cluster_count"
COMPUTATION_TASKS = "computation_tasks"
COMPUTATION_TASK_LIST = "computation_task_list"
EXCEPTION_TASK_LIST = "exception_task_list"
LAST_COMPUTE_DATE = "last_compute_date"
FIRST_COMPUTE_DATE = "first_compute_date"
USEFUL_CONTENT_COUNT = "useful_content_count"
EVENT_ID = "event_id"
TBS_ID = "tbs_id"
EVENT_IDS = "event_ids"
EVENT_CHUNK_ID = "event_chunk_id"
EVENTS = "events"
EVENT_COUNT = "event_count"
CONTENT_COUNT = "content_count"
CONTENT_SIZES = "content_sizes"
CONTENT_BATCHES = "content_batches"
CLUSTER_GROUP_COUNTS = "cluster_group_counts"
GROUP_CONTENT_COUNTS = "group_content_counts"
SESSION_ID = "session_id"
PARENT_SESSION_ID = "parent_session_id"
HANDLED_IDS = "handled_ids"
IDS = "ids"
IDS_COUNT = "ids_count"
SESSIONS = "sessions"
TEXT = "text"
TOKENS = "tokens"
SHINGLES = "shingles"
CONTENT_LANGUAGE = "content_language"
CONTENT = "content"
SMALL = "small"
NORMAL = "normal"
INLINE_CONTENT_TYPE = "inline"
FILENAME_CONTENT_TYPE = "filename"
SUBJECT_CONTENT_TYPE = "subject"
BIG = "big"
HASH = "hash"
CONTENT_ID = "content_id"
CONTENT_IDS = "content_ids"
FIELDS = "fields"
CONTENT_TYPE = "type"
CONTENT_TYPES = "content_types"
SIZE = "size"
SIZE_TYPE = "size_type"
TOKEN_COUNT = "token_count"
MIN_SIZE = "min_size"
MAX_SIZE = "max_size"
OBJECT_ID = "object_id"
CLUSTERS = "clusters"
CONTENTS_COUNT = "contents_count"
TAGS = "tags"
TAG_NAME = "tag_name"
TAG_INFLUENCE = "tag_influence"
TAG_INFLUENCE_NORM = "tag_influence_norm"
CONTENTS = "contents"
CLUSTER_DISTANCE = "cluster_distance"
CLUSTER_DISTANCE_NORM = "cluster_distance_norm"
CLUSTER_LABEL = "cluster_label"
LABEL = "label"
WORD = "word"
KEY = "key"
PARENT_CLUSTER_LABEL = "parent_cluster_label"
SESSION_PARAMS = "session_params"
ARCHIVE = "archive"
ARCHIVE_PATH = "archive_path"
SHINGLE_TYPE = "Type"
SHINGLE_DIGEST = "Digest"
SHINGLE_LENGTH = "Length"
SHINGLE_LOCATION = "Location"
SHINGLE_TOTAL_SIZE = "shingle_total_size"
NAMED_ENTITY_TYPE = "named_entity_type"
NAMED_ENTITY_NAME = "named_entity_name"
NAMED_ENTITIES = "named_entities"
GROUP_SIMILARITY = "group_similarity"
GROUPS = "groups"
GROUP = "group"
GROUP_CONTENT_BATHCES = "group_content_batches"
GROUP_ID = "group_id"
GROUP_DISTANCE = "group_distance"
GROUP_CONTENTS = "group_contents"
UNGROUPED = "ungrouped"
STATUS_CODE = "status_code"
STATUS_REASON = "reason"
FINISHED = "finished"
START = "start"
LIMIT = "limit"
SESSION_START_DATE = "session_start_date"
SESSION_END_DATE = "session_end_date"
SESSION_TYPE = "session_type"
QUERIES_ID = "queries_id"
TM_IDS = "tm_ids"
INT_IDS = "int_ids"
CHECK_ID = "check_id"
REQUEST_ID = "request_id"
REQUEST_NAME = "request_name"
TM_REQUESTS = "tm_requests"
TM_QUERIES_ID = "tm_queries_id"
HOST_WITH_ID = "host_with_id"
LAST_INSERT_DATE = "last_insert_date"
LAST_REQUEST_DATE = "last_request_date"
LAST_MODIFY_TIMESTAMP = "last_modify_timestamp"
VERSION = "version"
CONFIGURATION_VERSION = "configuration_version"
TM_EVENT_URL_LIST = "tm_event_url_list"
CHUNK_ID = "chunk_id"

KWARGS = "kwargs"
HOURS = "hours"
HOUR = "hour"
MINUTE = "minute"
WEEK_DAY = "week_day"
DAY_OF_WEEK = "day_of_week"
COMMAND = "command"
QUEUE_NAME = "queue_name"
AT = "at"
SCHEDULE_HOUR = "Hour"
SCHEDULE_DAY = "Day"
SCHEDULE_DAY_OF_WEEK = "DayOfWeek"
SCHEDULE_MINUTE = "Minute"
LOCALE = "locale"
AM = "AM"
PM = "PM"
EN = "en"
RU = "ru"
TIMEZONE = "timezone"
UTC = "utc"

THREAD_POOL_SIZE_ENV = "THREAD_POOL_SIZE"
PROCESS_POOL_SIZE_ENV = "PROCESS_POOL_SIZE"
PROCESS_POOL_SIZE_CONF = "process_pool_size"
PROCESS_CONTEXT_ENV = "PROCESS_CTX"
PROCESS_CONTEXT_CONF = "process_ctx"
LINUX_LINE_ENDING = "\n"
WINDOWS_LINE_ENDING = "\r\n"

RANGE = "range"

# константы для формирования конфига flask приложения
SRV_PASSWORD_ENV = "SRV_PASSWORD"
WORKER_NAME = "WorkerName"
WORKER_PASSWORD = "WorkerPassword"
SERVICE_DISCOVERY = "service_discovery"
SERVICE_DISCOVERY_CENTRAL = f"{SERVICE_DISCOVERY}_central"
SERVICE_DISCOVERY_LOCAL = f"{SERVICE_DISCOVERY}_local"
CUBE_CREDS = "cube_creds"
AUTH_REQUIRED_CONFIG = "AuthRequired"
CH_DB = "ch_db"
REDIS_DB = "redis_db"
PG_ENGINE = "pg_engine"
PG_READ_ONLY_ENGINE = "pg_read_only_engine"

# redis
REDIS_ENTITIES = "_entities"
DEFAULT_SCHEDULE = "default_schedule"
SCHEDULE = "schedule"
USER_SCHEDULE = "user_schedule"

# константы именования env переменных конфигурации
AUTH_REQUIRED_ENV = "AUTH_REQUIRED"
WORKERS_ENV = "WORKERS"
WORKERS_CONFIG = "Workers"

SCOPES = "scopes"
COMPANY = "company"
AGGREGATE = "aggregate"
DEPARTMENT_ID = "department_id"
INSTANCE = "instance"

CONDITION = "condition"
PATTERNS = "patterns"
PATTERN = "pattern"
RISK_GROUPS = "risk_groups"
RISK_GROUP = "risk_group"
RISK_GROUP_THRESHOLD = "risk_group_threshold"
RISK = "risk"
SUB_PATTERNS = "sub_patterns"
AGGREGATES = "aggregates"
AGGREGATES_TYPES = "aggregates_types"
PATTERN_ANNOTATIONS = "pattern_annotations"
PATTERN_MAPS = "pattern_maps"
PATTERN_MAP = "pattern_map"
CHAINS = "chains"
CHAIN = "chain"
STAGES = "stages"
STAGE = "stage"
ORDER = "order"
OFFSET = "offset"
DATE = "date"
FACT_DATE = "factDate"
FACT_DATE_TIME = "factDateTime"
TAG_STATUS = "tagStatus"
RELATED_IDS = "relatedIds"
TITLE = "title"
GROUP_CHILDREN_SHORT_ID = "groupChildrenShortId"
AGG_NAME = "agg_name"
AGG_FUNC = "agg_func"
PATTERN_IDS = "pattern_ids"
TOTAL = "total"
BY_DEPARTMENT = "by_department"
CALCULATE_CHILD_PATTERNS_SCORE = "calculate_child_patterns_score"
NEGATIVE_USER_ID = "negative_user_id"
NEGATIVE_PATTERNS = "negative_patterns"
NEGATIVE_RISK_GROUPS = "negative_risk_groups"
NEGATIVE_STATUS = "negative_status"
NEGATIVE_DEPARTMENT_ID = "negative_department_id"
NEGATIVE_AD_GROUP = "negative_ad_group"
NEGATIVE_POSITION = "negative_position"
NEGATIVE_INSTANCE = "negative_instance"
NEGATIVE_CONTACT = "negative_contact"
BY_RISK_GROUP_CONTROL_SECTION = "by_risk_group_control_section"
RISK_GROUP_CONTROL_SECTION = "risk_group_control_section"
RISK_GROUP_CONTROL_SECTION_THRESHOLD_HIGH = "risk_group_control_section_threshold_high"
RISK_GROUP_CONTROL_SECTION_THRESHOLD_MIDDLE = (
    "risk_group_control_section_threshold_middle"
)
RISK_GROUP_CONTROL_SECTION_THRESHOLD_LOW = "risk_group_control_section_threshold_low"
DRIVER_INSTANCE_PREDICTION = "driverInstancePrediction"
DRIVER_INSTANCE = "driverInstance"
DRIVER_INSTANCES = "driverInstances"
FILIALS = "filials"
NEGATIVE_FILIALS = "negative_filials"

QF_USER_ID = "id"
QF_PERSON_SCORE = "personScore"
QF_DEPARTMENT_SCORE = "departmentScore"
QF_PERSON_SCORE_PATTERNS = "personScoreWithPatterns"
QF_DEPARTMENT_SCORE_PATTERNS = "departmentScoreWithPatterns"
QF_STAT_FACTS = "statisticFacts"
QF_STAT_FACTS_DEP = "statisticFactsDepartment"
QF_STAT_WORKDAY = "statisticWorkday"
QF_PATTERN_MAPS_FACTS = "patternMapsFacts"
QF_PATTERN_MAPS_WORKDAY = "patternMapsWorkday"
QF_CHAINS_FACTS = "chainsFacts"
QF_CHAINS_WORKDAY = "chainsWorkday"
QF_PERSON_LINE_PLOTS = "personLinePlots"
QF_DEPARTMENT_LINE_PLOTS = "departmentLinePlots"
QF_PATTERN_MAPS_SCORE = "patternMapsScore"
QF_PATTERN_ANNOTATIONS = "patternAnnotations"
QF_CHAINS_SCORE = "chainsScore"
QF_START_OF_DAY = "StartOfDay"
FACTS = "facts"
RISKS = "risks"
PLOTS = "plots"
WORKDAY = "workday"
ATTACHMENTS_NAMES = "attachments_names"
ACTION_TYPE = "action_type"
FACT_ID_SOURCE = "fact_id_source"

STARTED = "started"
LAST_START = "last_start"
TASKS = "tasks"
TASK = "task"
TASK_DATES = "task_dates"

EXISTS = "exists"

##############################
# Команды служебных очередей #
##############################

# команда обновления ключа авторизации наших сервисов в CUBE
CUBE_SECRET_UPDATE = "service_cube_secret_update"
# команда обновления конфигурации
CONFIGURATION_UPDATED = "service_configuration_updated"
# Команда обновления списка активных пользователей
ENABLED_USERS_UPDATED = "enabled_users_updated"
# Команда обновления списка уволившихся пользователей
FIRED_USERS_UPDATED = "fired_users_updated"
# Команда обновления размера чанков пользователей
CHUNK_SIZE_UPDATED = "num_size_updated"
# Команда обновления модели прогнозирования увольнений
LP_MODEL_UPDATED = "lp_model_updated"
# Команда обновления информации о политиках
POLICIES_UPDATED = "policies_updated"
# Команда обновления параметров доступа к ТМ-агентам
TM_SOURCES_CREDS_UPDATED = "tm_sources_creds_updated"
# Команда обновления списка адресов автоматических запросов
AUTOREQUEST_UPDATED = "autorequest_updated"
# Команда обновления списка уведомлений об ухудшении рейтинга
NOTIFICATIONS_UPDATED = "notifications_updated"
# Команда повторной регистрации префикса
REGISTER_PREFIX = "register_prefix"
# Команда обновления списка нерабочих дней, полученных на основании анализа почтовой переписки
MAIL_NON_WORK_DAYS_UPDATED = "mail_non_work_days_updated"
# Команда обновления списка id связанных событий
PROTECTED_OBJECTS_UPDATED = "protected_objects_updated"
# Команда завершения процесса обновления списка нерабочих дней, полученных на основании анализа почтовой переписки
MAIL_NON_WORK_DAYS_COMPLETED = "mail_non_work_days_completed"
# Команда завершения процесса обновления списка id связанных событий
PROTECTED_OBJECTS_COMPLETED = "protected_objects_completed"
# Команда завершения процесса обновления информации о политиках
POLICIES_COMPLETED = "policies_completed"
# Команда завершения процесса обновления списка активных пользователей
ENABLED_USERS_COMPLETED = "enabled_users_completed"
# Команда завершения процесса обновления списка департаментов
DEPARTMENTS_COMPLETED = "departments_completed"
# Команда обновления списка департаментов
DEPARTMENTS_UPDATED = "departments_updated"
# Команда обновления расписания
SCHEDULE_UPDATED = "schedule_updated"
# Команда обновления шаблона
TEMPLATE_UPDATED = "template_updated"
# Команда завершения процесса обновления реплики досье
DOSSIER_REPLICA_COMPLETED = "dossier_replica_completed"
# Команда обновления реплики досье
DOSSIER_REPLICA_UPDATED = "dossier_replica_updated"

###############################

BOOL_TRUE_STRINGS = ("yes", "true", "t", "y", "1")
BOOL_FALSE_STRINGS = ("no", "false", "f", "n", "0")

Interval = namedtuple("Interval", ["start", "end"])


class TaskTypes(Enum):
    TODAY = "today"  # расчет на неполных данных за текущий день
    DAILY = "daily"  # перенос данных за день из тудей-таблиц в обычные таблицы
    HISTORY = "history"  # исторический расчет
    RECOMPUTE = "recompute"  # повторный расчет
    MAINTENANCE = "maintenance"  # вспомогательная задача
    NOTIF_PRECOMPUTE = (
        "notif_precompute"  # предварительный расчет нотификационных статистик
    )

    def __str__(self):
        return self.value


class TaskStatus(Enum):
    PENDING = 1
    STARTED = 2
    FINISHED = 3


def response_status(resp: dict) -> str:
    """Получить статус ответа"""
    return resp[STATUS]


def get_service_url(host: str, port: int, path: str = "") -> str:
    return f"http://{host}:{port}/{path}"


def serialize(data):
    return pickle.dumps(data)


def deserialize(serialized_data):
    return pickle.loads(serialized_data)


def load_json_data(file_path: str) -> Any:
    with open(file_path) as f:
        return json.load(f)


def load_text_data(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


def load_text_lines(file_path: str) -> list:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read().splitlines()


def load_config(conf_path: str) -> dict:
    conf = Path(conf_path)
    if not conf.exists():
        raise ut_exc.ConfigException(f"Config file '{conf_path}' was not found")

    with open(conf) as conf_file:
        return yaml.safe_load(conf_file)


def load_configuration(configuration_path: str) -> dict:
    configuration = Path(configuration_path)
    if not configuration.exists():
        raise ut_exc.ConfigException(
            f"Configuration file '{configuration_path}' was not found"
        )

    with open(configuration) as configuration_file:
        return json.load(configuration_file)


def get_conf_param(config, param_name: str, map_func):
    try:
        param_val = map_func(config.get(param_name))
    except TypeError:
        mes = f"Value for '{param_name}' is not defined in config"
        raise ut_exc.ConfigException(mes)
    except ValueError:
        mes = f"Param '{param_name}' can't be converted by {map_func}"
        raise ut_exc.ConfigException(mes)
    return param_val


def register_dependencies(
        pipeline_service_url: str, service_name: str, dependency_list: list
):
    """Зарегистрировать зависимости сервиса"""

    if not dependency_list:
        return

    register_info = {DEPENDENCIES: dependency_list}
    resp = requests.put(f"{pipeline_service_url}/{service_name}", json=register_info)
    resp.raise_for_status()


def delete_dependencies(pipeline_service_url: str, service_name: str):
    """Удалить зависимости сервиса"""

    resp = requests.delete(f"{pipeline_service_url}/{service_name}")
    resp.raise_for_status()


def register_prefix(pipeline_derived_url: str, service_name: str, prefix: str):
    register_info = {PREFIX: prefix}
    resp = requests.put(f"{pipeline_derived_url}/{service_name}", json=register_info)
    resp.raise_for_status()


def delete_prefix(pipeline_derived_url: str, service_name: str):
    resp = requests.delete(f"{pipeline_derived_url}/{service_name}")
    resp.raise_for_status()


def get_hash(data: Collection[str]):
    """Посчитать хэш отсортированного списка строк"""
    hasher = hashlib.md5()
    for s in sorted(data):
        hasher.update(s.encode("utf-8"))
    return hasher.hexdigest()


def get_dossier_replica_hash(dossier_data: list) -> str:
    hasher = hashlib.md5(json.dumps(dossier_data, sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


def grouper(iterable: Iterable, n: int) -> Generator:
    """
    Разбивает iterable на группы по n элементов,
    последнюю группу делает меньше n
    """
    for i in range(0, len(iterable), n):
        yield iterable[i: i + n]


def response_message(resp: requests.Response, ext_mes: Optional[str] = None) -> str:
    server_resp = f"Server response: '{resp.status_code} {resp.reason}: {resp.text}'"
    if ext_mes:
        return f"{ext_mes}. {server_resp}"
    return server_resp


def str_to_bool(bool_str: str) -> bool:
    if bool_str.lower() in BOOL_TRUE_STRINGS:
        return True
    elif bool_str.lower() in BOOL_FALSE_STRINGS:
        return False
    else:
        raise argparse.ArgumentTypeError(
            f"Boolean value expected. Use for 'True': {BOOL_TRUE_STRINGS}."
            f" For 'False': {BOOL_FALSE_STRINGS}"
        )


def parse_bool(var: Any):
    if isinstance(var, str):
        return str_to_bool(var)
    if isinstance(var, bool) or isinstance(var, int):
        return bool(var)
    raise ValueError(f"Parsing type '{type(var)}' to bool is not supported")


def parse_json(json_data: Union[str, dict, list, Path]) -> Union[dict, list]:
    if not isinstance(json_data, (str, dict, list, Path)):
        raise ValueError("Json data type must be one of (str, dict, list, Path)")

    if isinstance(json_data, (dict, list)):
        return json_data

    json_path = Path(json_data)
    if json_path.is_file():
        with open(json_path) as fp:
            return json.load(fp)
    return json.loads(json_data)


def bytes_to_base64(byte_data: bytes) -> str:
    return b64encode(byte_data).decode("ascii")


def base64_to_bytes(base64_str: str) -> bytes:
    return b64decode(base64_str.encode("ascii"))


def request_data(url: str) -> list:
    resp = requests.get(url)
    if resp.status_code != requests.codes.ok:
        return []
    return resp.json()


def normalize_date_time(task_type: str, date_time: str) -> str:
    """Нормализация времени для задач Prediction

    Значения полей дат в БД хранятся в округленном формате (YYYY-MM-DDT00:00:00).
    Статистики, рассчитываемые в течение дня, сохраняются с датой
    YYYY-MM-(DD + 1)T00:00:00 и перезаписываются при следующем расчете. Это позволяет
    существенно сократить объем хранимой информации.

    Для этих целей может потребоваться привести дату в задаче к нормализаванному виду:
    - для ежедневных задач - YYYY-MM-DDT00:00:00
    - для задач, приходящих в течение дня - YYYY-MM-(DD + 1)T00:00:00
    """
    date_time = datetime.strptime(date_time, DATE_FORMAT).replace(
        hour=0, minute=0, second=0
    )
    if task_type == TaskTypes.TODAY.value:
        date_time += timedelta(days=1)
    return date_time.strftime(DATE_FORMAT)


def get_filial():
    return os.getenv("SD_DISCOVERY_NODE")


################### PULSE ###################


ACTIVE_HOSTS = "active_hosts"
ACTIVITY_CONTENT_TYPE = "activity_content_type"

VECTORIZED_CONTENT = "vectorized_content"
CENTROID = "centroid"
CLUSTER_ID = "cluster_id"
PROTO_CLUSTER_ID = "proto_cluster_id"
PROTO_CLUSTER_IDS = "proto_cluster_ids"
PROTO_CLUSTERS = "proto_clusters"
PROTO_CLUSTER_COUNT = "proto_cluster_count"
MAX_SUBCLUSTER_COUNT = "max_subcluster_count"

MONGO_ID = "_id"
MONGO_FILES = "files"
MONGO_FILENAME = "filename"
MONGO_FILES_ID = "files_id"

QUERIES = "queries"

MIN_THRESHOLD_PERCENT = "min_threshold_percent"
MAX_THRESHOLD_PERCENT = "max_threshold_percent"

DATA_SIZE = "data_size"
STORAGE_SIZE = "storage_size"
INDEX_SIZE = "index_size"
TOTAL_SIZE = "total_size"
FREE_STORAGE_SIZE = "free_storage_size"
AVG_DOCUMENT_SIZE = "avg_document_size"
AVG_CONTENT_SIZE = "avg_content_size"
FS_USED_SIZE = "fs_used_size"
FS_TOTAL_SIZE = "fs_total_size"

SPACE_CLEANER_SCHEDULE_ELEMENT = "space_cleaner"
INIT_TASK_PARAMS = "init_task_params"


class ActivityContentType(str, Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"


class PulseTaskTypes(str, Enum):
    CLUST_BY_CONTENT_IDS = "clust_by_content_ids"
    CLUST_BY_QUERY_IDS = "clust_by_query_ids"
    RECLUSTERIZATION = "reclusterization"
    CHECK_CONNECTION = "check_connection"
    UPDATE_QUERIES = "update_queries"
    SCHEDULED_PREPROCESSING = "scheduled_preprocessing"
    MAINTENANCE = "maintenance"


class ConnectionCheckStatus(Enum):
    CHECK_DONE = (0, "OK")
    CHECK_IN_PROGRESS = (1, "Check in progress")
    NO_PING = (2, "Can't ping host")
    PUBLIC_API_ERROR = (3, "Can't access Public API")
    PRIVATE_API_ERROR = (4, "Can't access to Private API")
    DATA_VALIDATION_ERROR = (5, "Not valid parameters were provided")

    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description


def get_session_cluster_id(session_id: str, cluster_id: str) -> str:
    """Получить уникальный id кластера для сессии"""
    return f"{session_id}-{cluster_id}"
