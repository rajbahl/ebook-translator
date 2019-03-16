import logging
import aioamqp
import os
import json


logger = logging.getLogger("AmqpController")


class AmqpController:
    QOS_PREFETCH = 5

    def __init__(self):
        self._amqp_transport = None
        self._amqp_protocol = None
        self._loop = None
        self.amqp_conn = False
        self.amqp_chan = False
        self._routes = None             # { route_name: exchange_name }
        self._channels = None           # { exchange_name: channel_reference }
        self._queues = None             # { queue_name: route_name }
        self._pub_routes = None         # { route_name: exchange_name }
        self._meta = None
        self._exchange_types = {'products': 'topic', 'books': 'direct'}  # { exchange_name: exchange_type }

    def get_exchange_types(self) -> dict:
        return self._exchange_types

    def get_routes(self) -> dict:
        return self._routes

    def get_pub_routes(self) -> dict:
        return self._pub_routes

    def get_channel(self, name):
        if self._channels is not None:
            if self._channels[name]:
                return self._channels[name]
            else:
                return False
        else:
            return False

    def get_loop(self):
        return self._loop

    def set_loop(self, loop):
        self._loop = loop

    def set_exchange_types(self, exchange_types: dict):
        self._exchange_types = exchange_types

    def add_route(self, routes: dict):
        if self._routes is None:
            self._routes = dict()

        self._routes.update(routes)

    def add_pub_route(self, routes: dict):
        if self._pub_routes is None:
            self._pub_routes = dict()

        self._pub_routes.update(routes)

    def add_queue(self, queues: dict):
        if self._queues is None:
            self._queues = dict()

        self._queues.update(queues)

    async def add_channels(self):
        combined_routes = dict()
        if self._routes is not None:
            combined_routes.update(self._routes)
        if self._pub_routes is not None:
            combined_routes.update(self._pub_routes)

        for exchange_name in combined_routes.values():
            logger.info("Adding channel " + exchange_name)
            await self.add_amqp_channel(channel_name=exchange_name, exchange_name=exchange_name,
                                        exchange_type=self._exchange_types[exchange_name])

    async def consume_all(self):
        if self._queues is None or self._routes is None:
            logger.error("Queues empty.")
            return False
        else:
            for queue_name in self._queues.keys():
                for route in self._routes.keys():
                    if route == self._queues[queue_name]:  # queue is in this exchange.
                        logger.info("Consuming " + route + " and queue " + queue_name)
                        await self.consume(self._routes[route], queue_name)

    async def consume(self, channel_name, queue):
        if self._channels is not None:
            logger.info("Setting up channel " + channel_name + " with queue: " + str(queue))
            channel = self._channels[channel_name]
            try:
                await channel.basic_qos(prefetch_count=self.QOS_PREFETCH, prefetch_size=0)
                await channel.basic_consume(self.callback, queue_name=queue)
            except Exception as e:
                logger.error(e)
                raise Exception(e)
        else:
            return False

    async def publish(self, channel_name, exchange_name, message, routing_key=None, meta=None):
        message_dict = dict()
        message_dict["message"] = message

        if meta is not None:
            routing_key = meta.pop_path()
            additional_meta = dict()
            additional_meta["exchange"] = exchange_name
            additional_meta["routingkey"] = routing_key
            meta.set_labeled_time("PublishedAt", additional_meta)
            message_dict["meta"] = meta.get_meta()

        json_message = json.dumps(message_dict)
        logger.debug("Sending " + json_message + " to exchange " + exchange_name + " with route "
                     + routing_key)

        await self._channels[channel_name].publish(json_message, exchange_name=exchange_name, routing_key=routing_key)

    async def callback(self, channel, body, envelope, properties):
        logger.debug(body)
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def add_amqp_channel(self, channel_name, exchange_name, exchange_type):
        channel = await self._amqp_protocol.channel()
        channel.exchange_declare(exchange_name=exchange_name,
                                 type_name=exchange_type)

        if self._channels is None:
            self._channels = dict()

        self._channels[channel_name] = channel
        self.amqp_chan = True

    async def add_amqp_connection(self):
        amqp_host = None
        amqp_port = None
        try:

            amqp_host = os.getenv("AMQP_HOST", "rabbitmq.dev.muchneededllc.com")
            amqp_port = int(os.getenv("AMQP_PORT", 5672))

        except OSError as e:
            logger.error("Couldn't get environmental variables for amqp. " + str(e))
            exit(1)

        try:
            if self._loop is not None:
                self._amqp_transport, self._amqp_protocol = await aioamqp.connect(host=amqp_host,
                                                                                  port=amqp_port,
                                                                                  loop=self._loop)
            self.amqp_conn = True

        except aioamqp.AmqpClosedConnection:
            logger.error("closed connections")

    def cleanup(self):
        if self.amqp_chan:
            for chan in self._channels:
                self._channels[chan].close()
        if self.amqp_conn:
            self._amqp_protocol.close()
            self._amqp_transport.close()
