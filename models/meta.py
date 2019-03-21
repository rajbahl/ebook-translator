import logging
import json
from datetime import datetime


logger = logging.getLogger("MetaModel")


class Meta:
    def __init__(self, identifier: str = None, meta_dict: dict = None):
        self._meta = dict()
        self._path_vector = list()
        self._id = dict()
        self._meta["path"] = list()
        self._id["job_id"] = str()
        self._id["task_id"] = str()
        self._id["task_total"] = str()
        if meta_dict is None:
            self._timestamps = {"CreatedAt": {"CreatedAt": str(datetime.now())}}
            self._meta["timestamps"] = self._timestamps
            if identifier is None:
                self._id["job_id"] = str()
            else:
                self._id["job_id"] = identifier
            self._id["task_id"] = str()
            self._id["task_total"] = str()
            self._meta["id"] = self._id
        else:
            self.set_meta(meta_dict)

    def get_labeled_time(self, label):
        if label in self._timestamps:
            return self._timestamps[label]
        else:
            return False

    def get_path(self):
        return self._path_vector

    def get_meta(self):
        return self._meta

    def get_meta_json(self):
        return json.dumps(self._meta)

    def get_id(self):
        return self._id

    def get_job_id(self):
        return self._id["job_id"]

    def get_task_id(self):
        return self._id["task_id"]

    def get_task_total(self):
        return self._id["task_total"]

    def set_labeled_time(self, label: str, additional_meta: dict = None):
        if additional_meta is None:
            additional_meta = dict()

        additional_meta[label] = str(datetime.now())
        self._timestamps[label] = additional_meta
        logger.debug("labeled timestamps:" + str(self._timestamps))
        self._meta["timestamps"] = self._timestamps

    def set_path(self, vector: list):
        self._path_vector = vector
        self._meta["path"] = vector
        logger.debug("path:" + str(self._path_vector))

    def add_path(self, vector: list):
        self._path_vector.append(vector)
        self._meta["path"] = self._path_vector

    def del_path(self, vector: list):
        if vector in self._path_vector:
            self._path_vector.remove(vector)
            self._meta["path"] = self._path_vector

    def pop_path(self) -> str:
        if self._path_vector is not None:
            routing_key = self._path_vector.pop()
            self._meta["path"] = self._path_vector
            return routing_key
        else:
            logger.error("Error: No routes left in path")
            raise ValueError("Error: No routes left in path")

    def set_meta(self, meta: dict):
        self._meta = meta
        self._timestamps = self._meta["timestamps"]
        self._path_vector = self._meta["path"]
        self._id["job_id"] = self._meta["id"]["job_id"]
        self._id["task_id"] = self._meta["id"]["task_id"]
        self._id["task_total"] = self._meta["id"]["task_total"]

    def set_meta_json(self, json_meta: dict):
        self._meta = json.dumps(json_meta)
        self._timestamps = self._meta["timestamps"]
        self._path_vector = self._meta["path"]
        self._id["job_id"] = self._meta["id"]["job_id"]
        self._id["task_id"] = self._meta["id"]["task_id"]
        self._id["task_total"] = self._meta["id"]["task_total"]

    def set_id(self, identifier: dict):
        self._meta["id"] = identifier

    def set_job_id(self, identifier: str):
        self._id["job_id"] = identifier
        self._meta["id"] = self._id

    def set_task_id(self, identifier: str):
        self._id["task_id"] = identifier
        self._meta["id"] = self._id

    def set_task_total(self, total: str):
        self._id["task_total"] = total
        self._meta["id"] = self._id
