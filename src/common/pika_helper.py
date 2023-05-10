import select
import pika.exceptions

CONNECTIVITY_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ChannelClosed,
    select.error,
)
