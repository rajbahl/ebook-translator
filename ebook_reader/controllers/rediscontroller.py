from aredis import StrictRedis
import os
import logging


logger = logging.getLogger("RedisController")


class RedisController:
    def __init__(self):
        self._redis_client = None
        self.redis_conn = False
        self._loop = None

    def get_loop(self):
        return self._loop

    def get_redis_client(self):
        return self._redis_client

    async def get_value(self, key: str) -> str:
        try:
            assert await self._redis_client.exists(key) is True
            result = await self._redis_client.get(key)
        except AssertionError as e:
            logger.error("Redis client doesn't exist when it should. " + str(e))
            result = ""

        result = result.decode('utf-8')
        return result

    def set_loop(self, loop):
        self._loop = loop

    def add_redis_client(self):
        kvstore_host = None
        kvstore_port = None
        kvstore_db = None
        try:

            kvstore_host = os.getenv("KVSTORE_HOST", "redis.dev.muchneededllc.com")
            kvstore_port = int(os.getenv("KVSTORE_PORT", 6379))
            kvstore_db = int(os.getenv("KVSTORE_DB", 0))

        except OSError as e:
            logger.error("Couldn't get environmental variables for kvstore. " + str(e))
            exit(1)

        try:
            if self._loop is not None:
                self._redis_client = StrictRedis(host=kvstore_host, port=kvstore_port, db=kvstore_db, loop=self._loop)
                self.redis_conn = True
                logger.debug("Created Redis Client.")
            else:
                logger.error("Couldn't create redis client because loop hasn't been set.")

        except Exception as e:
            logger.error("couldn't open redis.")
            raise Exception(e)

    def cleanup(self):
        if self.redis_conn:
            self._redis_client.close()
