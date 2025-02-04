from utils.pulse_sessions import SessionStatus


class BasicException(Exception):
    """ Базовый класс исключений библиотеки """

    def __init__(self, message):
        super(BasicException, self).__init__(message)
        self._message = message

    @property
    def message(self):
        return self._message


class ConfigException(BasicException):
    """Ошибка разбора файлов конфигурации"""


class WrongDateFormatException(BasicException):
    """Ошибка формата даты"""


class BadRequestException(BasicException):
    """Ошибка выполнения запроса"""


class PerformTaskException(BasicException):
    """Ошибка обработки задачи

    Исключение можно выбросить в процессе обработки задачи (в методе _perform_task),
     чтобы вернуть ее обратно в очередь
    """


class BaseSessionException(BasicException):
    """Базовый класс исключений, передающих статус сессии"""

    def __init__(self, status: SessionStatus, message: str):
        super(BaseSessionException, self).__init__(message)
        self._status = status

    @property
    def status(self):
        return self._status


class SessionPerformTaskException(BaseSessionException):
    """Ошибка обработки задачи с обновлением статуса сессии

    Исключение можно выбросить в процессе обработки сессионной задачи
     (в методе _perform_task), чтобы вернуть ее обратно в очередь
    """


class SessionTaskHandleException(BaseSessionException):
    """Ошибка обработки задачи с обновлением статуса сессии

    Исключение можно выбросить в процессе обработки сессионной задачи
     (на любом из этапов обработки задачи), чтобы вернуть ее обратно в очередь
    """


class SessionStopPipelineException(BaseSessionException):
    """Ошибка обработки задачи с обновлением статуса сессии и остановкой пайплайна сессии

    Исключение можно выбросить в процессе обработки сессионной задачи
     (на любом из этапов обработки задачи), чтобы отбросить ее и остановить пайплайн
    """
