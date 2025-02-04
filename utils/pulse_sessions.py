from enum import Enum


class SessionType(Enum):
    USER_SESSION = 0
    SCHEDULE_SESSION = 1
    RECLUSTERIZATION_SESSION = 2


class SessionStatus(Enum):
    SESSION_COMPLETED = (0, "Session completed")
    INITIALIZATION = (1, "Session initialization")
    EVENTS_INTEGRATION = (2, "Events integration")
    FEATURE_EXTRACTION = (3, "Feature extraction")
    CLUSTERIZATION = (4, "Clusterization")
    GROUPING = (5, "Grouping")
    RESULTS_ARCHIVATION = (6, "Results archivation")

    NO_EVENTS = (200, "No events found")
    NO_CONTENTS_FOUND = (201, "No contents found")
    NOT_ENOUGH_CONTENTS = (202, "Not enough contents")
    TOO_MANY_CONTENTS = (203, "Too many contents")
    BAD_INTERVAL = (204, "Bad interval")
    BAD_CLUSTER_COUNT = (205, "Bad cluster count")
    STOPPED = (206, "Session was stopped")

    CONNECTION_ERROR = (500, "Connection Error")
    DB_ERROR = (501, "Database Error")
    INTERNAL_ERROR = (502, "Internal Error")

    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description
