import json
from datetime import datetime
from time import sleep
import pika
import structlog
from src import settings
from src.common.mdb import db_source
from src.common.pika_helper import CONNECTIVITY_ERRORS

_logger = structlog.get_logger()


class ChannelPublisher:
    EXCHANGE = 'parse_channel_article'
    TYPE = 'fanout'

    def __init__(self):
        self._params = pika.connection.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            credentials=pika.credentials.PlainCredentials(settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD)
        )
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(self._params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.EXCHANGE,
            exchange_type=self.TYPE
        )

    def _publish(self, msg):
        self.channel.basic_publish(exchange=self.EXCHANGE, body=json.dumps(msg).encode(), routing_key='')

    def parse_channel(self, url: str, source_id):
        msg = {'url': url, 'source_id': str(source_id)}
        try:
            self._publish(msg)
            _logger.msg(f'[x] Sent message', msg=msg)
        except CONNECTIVITY_ERRORS:
            _logger.exception('Reconnecting to queue')
            self.connect()
            self._publish(msg)
            _logger.msg(f'[x] Sent message', msg=msg)

    def close(self):
        if self.connection and self.connection.is_open:
            _logger.msg('Closing queue connection')
            self.connection.close()


if __name__ == '__main__':

    channel_publisher = ChannelPublisher()
    channel_publisher.connect()

    source_to_last_run_at = {}
    while True:
        try:
            sources = list(db_source.find({'enable': True}).sort('_id', -1))
            _logger.msg(f'[x] Processing all sources', countSource=len(sources))

            for source in sources:
                if source.get('sourceType') == 1:
                    last_run_at = source_to_last_run_at.get(source.get('_id'))
                    if not last_run_at or (datetime.now() - last_run_at).total_seconds() > source.get('intervalSecond'):
                        _logger.msg(f"Processing source", sourceId=source.get('_id'))
                        source_to_last_run_at[source.get('_id')] = datetime.now()
                        if source.get('channels'):
                            for channel_url in source.get('channels'):
                                channel_publisher.parse_channel(channel_url, source.get('_id'))
                        else:
                            channel_publisher.parse_channel(source.get('url'), source.get('_id'))

            _logger.msg('Sleeping ... ')
            sleep(5)
        except Exception:
            _logger.exception('Error occurred')
