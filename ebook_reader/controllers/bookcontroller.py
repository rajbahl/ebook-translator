from controllers.amqpcontroller import AmqpController
from controllers.s3controller import S3Controller
from controllers.ebookcontroller import EbookController
from models.meta import Meta
import logging
import json
import copy

logger = logging.getLogger("BookController")


class BookController(AmqpController, S3Controller):
    _BOOK_EXCHANGE = 'books'
    _BOOK_ROUTE = 'translation.book.order'

    def __init__(self):
        AmqpController.__init__(self)
        S3Controller.__init__(self)
        self._meta = dict()
        self._meta["app"] = Meta()

    async def callback(self, channel, body, envelope, properties):
        body_dict = json.loads(body)
        identifier = body_dict["meta"]["id"]["job_id"]
        self._meta[identifier] = Meta(meta_dict=body_dict["meta"])
        self._meta[identifier].set_labeled_time('ReceivedAt')
        message = json.loads(body_dict["message"])
        location = message[identifier]["location"]
        bucket = message[identifier]["bucket"]
        logger.debug("meta_json: " + str(self._meta[identifier].get_meta_json()))

        await self.download_file(bucket=bucket, location=location)
        self._meta[identifier].set_labeled_time('DownloadedAt')
        ebook = EbookController()
        await ebook.read_book(location)
        finished_book = await ebook.get_parsed_book()
        self._meta[identifier].set_labeled_time('FinishedBookAt')

        book_length = 0
        for key in finished_book.keys():
            book_length += len(finished_book[key])

        logger.debug("Book Length: " + str(book_length))
        count = 1
        for key in finished_book.keys():
            for line in finished_book[key]:
                meta = copy.deepcopy(self._meta[identifier])
                meta.set_task_id(str(count))
                meta.set_task_total(book_length)
                identifier = meta.get_job_id()
                new_message = {identifier: line}
                await self.publish(channel_name=self._BOOK_EXCHANGE, exchange_name=self._BOOK_EXCHANGE,
                                   message=new_message, meta=meta)
                count += 1
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
