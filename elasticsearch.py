    # -*- coding: utf-8 -*-
"""
Elasticsearch implementation using asyncio python library

"""

import asyncio

from aioes import Elasticsearch
from aiohttp import web

import os
import logging


logger = logging.getLogger("ElasticsearchController")


class ElasticsearchController:
    def __init__(self):
        self._elasticsearch = None
        self.elasticsearch_conn = False
        self._loop = None
        
    def get_loop(self):
        return self._loop

    def get_elasticsearch_client(self):
        return self._elasticsearch_client

    async def get_value(self, index: str, id: str, doc_type='_all') -> str:
        try:
            assert await self._elasticsearch_client.exists(index, id) is True
            result = await self._elasticsearch_client.get(index, id)
        except AssertionError as e:
            logger.error("Elasticsearch client doesn't exist when it should. " + str(e))
            result = ""

        result = result.decode('utf-8')
        return result

    def set_loop(self, loop):
        self._loop = loop

    def add_elasticsearch_client(self):
        elasticsearch_host = None
        elasticsearch_port = None
        try:

            elasticsearch_host = os.getenv("ELASTICSEARCH_HOST", "redis.dev.muchneededllc.com")
            elasticsearch_port = str(os.getenv("ELASTICSEARCH_PORT", 9200))
            
        except OSError as e:
            logger.error("Couldn't get environmental variables for elasticearch. " + str(e))
            exit(1)

        try:
            if self._loop is not None:
                address = ':'.join([elasticsearch_host, elasticsearch_port])
                self._elasticsearch_client = Elasticsearch([address], loop=self._loop)
                self.elasticsearch_conn = True
                logger.debug("Created Elasticsearch Client.")
            else:
                logger.error("Couldn't create elasticsearch client because loop hasn't been set.")

        except Exception as e:
            logger.error("couldn't open elasticsearch.")
            raise Exception(e)

    def cleanup(self):
        if self.elasticsearch_conn:
            self._elasticsearch_client.close()

