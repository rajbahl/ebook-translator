    # -*- coding: utf-8 -*-
"""
Elasticsearch implementation using asyncio python library

"""

import asyncio

from aioes import Elasticsearch
from aiohttp import web


class AioESService:
    """
    Adaptor class to elasticsearch database using aioes Library.
    It encapsulate connection to elasticsearh database.
    """
    def __init__(self, loop=None):
        self.es = Elasticsearch(["127.0.0.1:9200"], loop=loop)
    
    
    async def get_info(self):
        """
        Get elasticsearch database information from cluster
        """
        return await self.es.info()
    
    
    async def get(self, index, id,  doc_type='_all'):
        """
        Get JSON document from the cluster based on ID
        """
        return await self.es.get(index, id, doc_type=doc_type)
    
    
    async def create(self, index, doc_type, body, id=None ):
        """
        Add JSON document in a specific index 
        """
        return await self.es.create(index, doc_type, body, id=id)
    
    
    async def exists(self, index, id, doc_type='_all' ):
        """
        Return boolean indicating whether or not given document exists
        """
        return await self.es.exists(index, id, doc_type=doc_type)
    
    
    async def update(self, index, doc_type, id, body = None):
        """
        Update document by provided data
        """
        return await self.es.update(index, doc_type, id, body=body)
    
    
    async def search(self, index=None, doc_type=None, body=None):
        """
        It execute the query and get the results back that matches the query
        """
        return await self.es.search(index=index, doc_typ=doc_type, body=body)
    
    
    async def delete(self, index, doc_type, id ):
        """
        Delete JSON document from index by ID
        """
        return await self.es.delete(index, doc_type, id)
    
    
    async def count(self, index=None, doc_type=None, body=None):
        """
        It execute query and return total number of matches for the query
        """
        return await self.es.count(index=index, doc_type=doc_type, body=body)


class ESBase:
    """
    Implementation of elasticsearch adaptor class.
    this is just how to use elasticsearch with asyncio.
    """
    objects = AioESService()
            


async def hello_aioes(request):
    """
    Web based interface to elasticsearch implementation.
    """
    cluster_info = await ESBase.objects.get_info()
    second = await ESBase.objects.count()
    return web.Response(text="{} \n {}".format(cluster_info, second))


def create_app(loop=None):
    """
    Web app for the asyncio elasticsearch implementation
    """
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', hello_aioes)
    return app


if __name__ == "__main__":
    web.run_app(create_app())