from config import app
from config import book
from controllers.bookcontroller import BookController
import asyncio
import aiotask_context as context
import uvloop
import logging
import os


logger = logging.getLogger("Main")


def main():
    level = int(os.getenv("LOGGING_LEVEL", logging.DEBUG))
    # if 0 <= level < 6:
    logging.basicConfig(filename='access.log', filemode='w', level=level)
    logger.debug("Logging level set to " + str(level) + " for " + app.get_app_name())

    routes = {'processing.book.order': 'books'}            # { route: exchange }
    pub_routes = {'translation.book.order': 'books'}       # { route: exchange }
    exchange_types = {'books': 'direct'}                   # { exchange: exchange_type }

    queues = {'processing.book.order': 'processing.book.order'}  # { queue_name: route }
    book.add_route(routes)
    book.add_pub_route(pub_routes)
    book.set_exchange_types(exchange_types)
    book.add_queue(queues)
    asyncio.set_event_loop(uvloop.new_event_loop())
    loop = asyncio.get_event_loop()

    book.set_loop(loop)
    loop.run_until_complete(book.add_elasticsearch_connection())
    loop.run_until_complete(book.add_s3_connection())

    loop.run_until_complete(book.add_amqp_connection())
    loop.run_until_complete(book.add_channels())

    loop.set_task_factory(context.task_factory)
    task = asyncio.ensure_future(book.consume_all())
    try:

        loop.run_forever()

    except OSError as e:
        logger.error("Unable to bind to port. " + str(e))
        loop.stop()
        exit(1)
    except Exception as e:
        loop.stop()
        logger.error("Error:" + str(e))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        book.cleanup()
        loop.close()


if __name__ == '__main__':
    main()
