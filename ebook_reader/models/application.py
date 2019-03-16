import logging
import uuid


logger = logging.getLogger("Connections")


class Application:
    def __init__(self, app_name):
        self._app_name = app_name
        self._app_id = uuid.uuid4()

    def get_id(self):
        return self._app_id

    def get_app_name(self):
        return self._app_name

    def set_app_name(self, app_name):
        self._app_name = app_name