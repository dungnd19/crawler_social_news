import functools
import json
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import sleep
from urllib.parse import urlparse

import pika
import structlog
from bson import ObjectId
from newspaper import Article
from pika.exchange_type import ExchangeType
from pytz import utc, timezone
from newspaper import Config

from src import settings
from src.common.extractor import CustomExtractor
from src.common.mdb import db_article
from src.common.helpers import get_creator
from src.common.pika_helper import CONNECTIVITY_ERRORS

_logger = structlog.get_logger()


class ArticleParser:
    PARSE_ARTICLE_QUEUE = 'parse_article'
    PARSE_ARTICLE_EXCHANGE = 'parse_article'

    DOWNLOAD_MEDIA_EXCHANGE = 'download_media'

    def __init__(self):
        self.shutdown_signal = False
        self._params = pika.connection.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            credentials=pika.credentials.PlainCredentials(settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD)
        )

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.connection = None
        self.thread_pool = ThreadPoolExecutor(max_workers=settings.THREAD_NUM)
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(self._params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=self.PARSE_ARTICLE_QUEUE,
            auto_delete=True
        )

        self.channel.queue_bind(self.PARSE_ARTICLE_QUEUE, self.PARSE_ARTICLE_EXCHANGE)
        self.channel.basic_qos(prefetch_count=100)

    def _download_media(self, url: str, article_id: int):
        _logger.msg("Sent message", url=url, exchange=self.DOWNLOAD_MEDIA_EXCHANGE)
        publish_connection = pika.BlockingConnection(self._params)
        publish_channel = publish_connection.channel()
        publish_channel.exchange_declare(
            self.DOWNLOAD_MEDIA_EXCHANGE,
            exchange_type=ExchangeType.fanout.value,
            durable=True
        )

        publish_channel.basic_publish(
            exchange=self.DOWNLOAD_MEDIA_EXCHANGE,
            body=json.dumps({'url': url, 'article_id': str(article_id)}).encode(),
            routing_key='',
            properties=pika.BasicProperties(delivery_mode=2)
        )
        publish_connection.close()

    def _handle_callback(self, ch, delivery_tag, body):
        msg = json.loads(body)
        url = msg.get('url')
        source_id = msg.get('source_id')

        _logger.msg("[x] Received message", msg=msg)

        try:
            config = Config()
            config.request_timeout = settings.REQUEST_TIMEOUT
            config.browser_user_agent = settings.USER_AGENT
            parsed_article = Article(url=url, language='vi', config=config)
            parsed_article.extractor = CustomExtractor(parsed_article.config)
            parsed_article.download()
            parsed_article.parse()

            if not parsed_article.publish_date:
                _logger.msg("Cannot extract origin_created_at", msg=msg)
                return
            else:
                published_date: datetime = parsed_article.publish_date

                if published_date.tzinfo is None:  # if datetime has no timezone, set to gmt+7
                    published_date = timezone('Asia/Ho_Chi_Minh').localize(published_date)

                published_date = published_date.astimezone(utc).replace(tzinfo=None)

            article = {
                'url': url,
                'path': urlparse(url).path,
                'source_id': ObjectId(source_id),
                'title': parsed_article.title,
                'origin_created_at': published_date,
                'description': parsed_article.meta_description,
                'content': parsed_article.text,
                'image_url': parsed_article.top_img,
                'keywords': parsed_article.meta_keywords,
                'status': 1
            }

            old_article = db_article.find_one({'path': article.get('path'),
                                               'source_id': article.get('source_id')})
            if not old_article:
                article['updated_at'] = datetime.utcnow()
                article['created_at'] = datetime.utcnow()
                article['creator_id'] = get_creator().get('_id')
                created_article = db_article.insert_one(article)
                _logger.msg("Created new article", article_id=created_article.inserted_id)
                self._download_media(url=article.get('image_url'), article_id=created_article.inserted_id)
            else:
                _logger.msg("Existed article", source_id=article.get('source_id'),
                            path=article.get('path'))

        except Exception as e:
            _logger.exception(f'Error when processing message', msg=msg)
        finally:
            cb = functools.partial(self._ack_message, ch, delivery_tag)
            ch.connection.add_callback_threadsafe(cb)

    def _ack_message(self, ch, delivery_tag):
        if ch.is_open:
            ch.basic_ack(delivery_tag)

    def _callback(self, ch, method, properties, body):
        try:
            delivery_tag = method.delivery_tag
            self.thread_pool.submit(self._handle_callback, ch, delivery_tag, body)
        except Exception as e:
            _logger.exception("Error when executing callback", body=body)

    def start_worker(self):
        self.connect()

        while not self.shutdown_signal:
            try:
                _logger.msg('[x] ArticleParser started consuming')
                self.channel.basic_consume(queue=self.PARSE_ARTICLE_EXCHANGE, on_message_callback=self._callback)
                self.channel.start_consuming()
            except CONNECTIVITY_ERRORS:
                sleep(1)
                _logger.exception('Reconnecting to queue')
                self.connect()
            except Exception:
                _logger.exception('Error occurred')

    def close(self):
        if self.connection and self.connection.is_open:
            _logger.msg('Closing connection')
            self.connection.close()

    def exit_gracefully(self, signal_no, stack_frame):
        self.shutdown_signal = True
        self.close()
        self.thread_pool.shutdown()
        raise SystemExit


if __name__ == '__main__':
    article_parser = ArticleParser()
    article_parser.start_worker()
