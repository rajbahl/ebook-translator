    # -*- coding: utf-8 -*-
"""
Elasticsearch implementation using asyncio python library

"""

from functools import wraps
import asyncio

from aioes import Elasticsearch
from aiohttp import web

import os
import logging
from datetime import datetime



logger = logging.getLogger("ElasticsearchController")

def timestamped(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = datetime.now()
        result = await func(*args, **kwargs)
        end = datetime.now()
        return {'started_at': start, 'ended_at': end, 'result': result}
    return wrapper



class ElasticsearchController:
    def __init__(self):
        self._elasticsearch = None
        self.elasticsearch_conn = False
        self._loop = None
        
    def get_loop(self):
        return self._loop

    def get_elasticsearch_client(self):
        return self._elasticsearch_client

    @timestamped
    async def get_value(self, index: str, id: str, doc_type='_all'):
        """
        Get record from elasticsearch using id and index
        :param index: used to search the named index for records
        :param id: used to find record in the index
        :param doc_type:
        :return: []
        """
        try:
            assert await self.exists(index, id) is True
            result = await self._elasticsearch_client.get(index, id)
        except AssertionError as e:
            logger.error("Elasticsearch client doesn't exist when it should. " + str(e))
            result = ""

        result = result.decode('utf-8')
        return result

    @timestamped
    async def create(self, index, doc_type, body, id=None):
        """
        Used to create new record in the elasticsearch database
        :param index: used to create in that specific index
        :param doc_type: specify elasticsearch document type
        :param body: actual body of the record to be created in the database
        :return: Json object
        """
        return await self._elasticsearch_client.create(index, doc_type, body, id=42)

    async def exists(self, index, id):
        """
        Used to check if record exist in the elasticsearch database using id and index
        :param index: search index for record
        :param id: find record for the id
        :return:
        """
        return await self._elasticsearch_client.exists(index, id)

    @timestamped
    async def update(self, index, doc_type, id, body=None):
        """
        Used to update record in the elasticsearch database
        :param index: select index to update record in that index
        :param doc_type: specify elasticsearch document type
        :param id: Identifier for the record to be updated
        :param body: the actual body for the record
        :return:
        """
        return await self._elasticsearch_client.update(index, doc_type, id, body=body)

    @timestamped
    async def search(self, index=None, doc_type=None, body=None):
        """
        Used to search for the record in the elasticsearch database
        :param index: used to search the index
        :param doc_type:
        :param body: query to be executed to match the result
        :return:
        """
        return await self._elasticsearch_client.search(index=index, doc_type=doc_type, body=body)

    @timestamped
    async def delete(self, index, doc_type, id):
        """
        Used to delete record from elasticsearch database
        :param index: specify the index for the record to be deleted
        :param doc_type:
        :param id: specify the id for the record to be deleted
        :return:
        """
        return await self._elasticsearch_client.delete(index, doc_type, id)

    def set_loop(self, loop):
        self._loop = loop

    @timestamped
    async def add_elasticsearch_connection(self):
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

                logger.info(self._elasticsearch_client)
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

