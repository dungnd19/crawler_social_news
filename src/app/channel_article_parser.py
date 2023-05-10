import functools
import json
import signal
import threading
from time import sleep
from urllib.parse import urlparse, urljoin
import pika
import requests as requests
import structlog
from bs4 import BeautifulSoup
from pika.exchange_type import ExchangeType

from src import settings
from src.common.pika_helper import CONNECTIVITY_ERRORS

_logger = structlog.get_logger()


class ChannelArticleParser:
    PARSE_CHANNEL_ARTICLE_EXCHANGE = 'parse_channel_article'
    PARSE_CHANNEL_ARTICLE_QUEUE = 'parse_channel_article'
    PARSE_ARTICLE_EXCHANGE = 'parse_article'
    PARSE_ARTICLE_TYPE = 'fanout'

    def __init__(self):
        self._params = pika.connection.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            credentials=pika.credentials.PlainCredentials(settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD)
        )
        self.connection = None
        self.channel = None

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.shutdown_signal = False

    def connect(self):
        self.connection = pika.BlockingConnection(self._params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.PARSE_CHANNEL_ARTICLE_EXCHANGE, auto_delete=True)
        self.channel.queue_bind(self.PARSE_CHANNEL_ARTICLE_QUEUE, self.PARSE_CHANNEL_ARTICLE_EXCHANGE)
        self.channel.exchange_declare(self.PARSE_ARTICLE_EXCHANGE, exchange_type=ExchangeType.fanout.value)
        self.channel.basic_qos(prefetch_count=settings.THREAD_NUM)

    @staticmethod
    def _base_url(url):
        base_url = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))
        return base_url

    def _parse_article(self, msg):
        publish_connection = pika.BlockingConnection(self._params)
        publish_channel = publish_connection.channel()
        publish_channel.basic_publish(exchange=self.PARSE_ARTICLE_EXCHANGE, body=json.dumps(msg).encode(),
                                      routing_key='')
        publish_connection.close()

    def _handle_callback(self, ch, delivery_tag, body):
        msg = json.loads(body)
        _logger.msg("[x] Received message", msg=msg)
        url = msg.get('url')
        source_id = msg.get('source_id')

        try:
            resp = requests.get(url, timeout=settings.REQUEST_TIMEOUT)
            if resp.ok:
                soup = BeautifulSoup(resp.text, features="html.parser")
                elements = soup.findAll('a')
                for element in elements:
                    href = element.get('href')
                    if href and len(href) > 40:
                        article_url = urljoin(self._base_url(url), href)
                        if self._base_url(article_url) == self._base_url(url):
                            msg = {'url': article_url, 'source_id': source_id}
                            try:
                                self._parse_article(msg)
                                _logger.msg(f'[x] Sent message', msg=msg)
                            except CONNECTIVITY_ERRORS:
                                _logger.exception('Reconnecting to queue')
                                self.connect()
                                self._parse_article(msg)
                                _logger.msg(f'[x] Sent message', msg=msg)

                            sleep(0.05)
            else:
                _logger.msg(f'Error when processing message', msg=msg, body=resp.text, status_code=resp.status_code)
        except Exception:
            _logger.exception(f'Error when processing message', msg=msg)
        finally:
            cb = functools.partial(self._ack_message, ch, delivery_tag)
            ch.connection.add_callback_threadsafe(cb)

    def _callback(self, ch, method, properties, body, threads):
        try:
            delivery_tag = method.delivery_tag
            t = threading.Thread(target=self._handle_callback, args=(ch, delivery_tag, body))
            t.start()
            threads.append(t)
        except Exception as e:
            _logger.exception("Error when executing callback", body=body)

    def _ack_message(self, ch, delivery_tag):
        if ch.is_open:
            ch.basic_ack(delivery_tag)

    def start_worker(self):
        self.connect()

        while not self.shutdown_signal:
            try:
                _logger.msg('[x] ChannelArticleParser started consuming')
                threads = []
                callback = functools.partial(self._callback, threads=threads)
                self.channel.basic_consume(queue=self.PARSE_CHANNEL_ARTICLE_QUEUE, on_message_callback=callback)
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
        raise SystemExit


if __name__ == '__main__':
    channel_article_parser = ChannelArticleParser()
    channel_article_parser.start_worker()
