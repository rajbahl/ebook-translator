    # -*- coding: utf-8 -*-
"""
Class to connect with elasticsearch using asyncio python library

"""

import asyncio

from aioes import Elasticsearch
from aiohttp import web


class AioESService:
    
    def __init__(self, loop=None):
        self.es = Elasticsearch(["127.0.0.1:9200"], loop=loop)
    
    async def get_info(self):
        return await self.es.info()
    
    async def get(self, id=None, index=None, doc_type=None):
        return await self.es.get(index=index, doc_type=doc_type, id=id)
    
    async def create(self, id, index=None, doc_type=None, body=None ):
        return await self.es.create(index=index, doc_type=doc_type, id=id, body=body)
    
    async def exists(self, id, index=None, doc_type=None ):
        return await self.es.exists(index, id, doc_type=doc_type)
    
    async def update(self, id, index=None, doc_type=None, body = None):
        return await self.es.update(index, doc_type, id, body=body)
    
    async def search(self, index=None, doc_type=None, body=None):
        return await self.es.search(index = index, doc_typ=doc_type, body=body)
    
    async def delete(self, id, index=None, doc_type=None ):
        return await self.es.delete(index, doc_type, id)
    
    async def count(self):
        return await self.es.count()


class ESBase:
    objects = AioESService()
            

async def hello_aioes(request):
    cluster_info = await ESBase.objects.get_info()
    second = await ESBase.objects.get_info()
    return web.Response(text="{} {}".format(cluster_info, second))


def create_app(loop=None):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', hello_aioes)
    return app


if __name__ == "__main__":
    web.run_app(create_app())