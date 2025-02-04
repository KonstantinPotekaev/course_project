from enum import Enum


class StatusCodes(Enum):

    WAITING = (100, "Waiting")
    IN_PROGRESS = (101, "In Progress")

    OK = (200, "Ok")

    WRONG_MESSAGE_TYPE = (400, "Wrong message type")
    WRONG_PARAMETER_VALUE = (401, "Wrong parameter value")

    # Статус указывает на ошибку, вызванную состоянием окружения
    # в момент выполнения запроса. Предполагает возможность повторной отправки
    INTERNAL_ERROR = (500, "Internal Error")
    DB_ERROR = (501, "Database Error")
    CONNECTION_ERROR = (502, "Connection Error")
    STOP_SUBSCRIBER_ERROR = (503, "Stop subscriber error")
    SLOW_CONSUMER_ERROR = (505, "Slow consumer")

    # Проблема в анализируемом контенте. Отправлять повторно не имеет смысла
    CONTENT_NOT_FOUND = (600, "Missing content error")
    NO_TOKENS_FOUND = (601, "Missing tokens error")
    NULL_VECTOR = (602, "Null vector error")
    BROKEN_CONTENT_ERROR = (603, "Broken content error")

    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description

    @classmethod
    def by_code(self, code: int) -> 'StatusCodes':
        return next((status for status in self if status.code == code), self.INTERNAL_ERROR)
