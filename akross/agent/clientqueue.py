import functools
import logging
from pika.exchange_type import ExchangeType


LOGGER = logging.getLogger(__name__)


class ClientQueue(object):
    def __init__(self,
                 connection,
                 _provider_outlet,
                 *,
                 exchange_name = '',
                 exchange_type = ExchangeType.headers,
                 routing_key = '',
                 headers = None
    ):
        self._connection = connection
        self.provider_outlet = _provider_outlet
        self._channel = None
        self._prefetch_count = 1
        self.is_connection_closed = False
        self._queue_name = None
        self._consuming = False
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._routing_key = routing_key
        self._headers = headers

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.get_connection().channel(
            on_open_callback=self.on_channel_open)

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s(%s) from %s: %s',
                    basic_deliver.delivery_tag,
                    basic_deliver.routing_key,
                    properties.app_id,
                    body)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange()

    def setup_exchange(self):
        LOGGER.info('Declaring exchange: %s', self._exchange_name)
        cb = functools.partial(
            self.on_exchange_declareok, userdata=self._exchange_name)
        self._channel.exchange_declare(
            exchange=self._exchange_name,
            exchange_type=self._exchange_type,
            auto_delete=True, #TODO: True???
            callback=cb)

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.is_connection_closed = True
        self._connection.close_connection()

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue()

    def setup_queue(self):
        self._channel.queue_declare('',
                                    auto_delete=True,
                                    exclusive=True,
                                    callback=self.on_queue_declareok)
        LOGGER.info('Declaring queue %s', self._queue_name)

    def on_queue_declareok(self, method_frame):
        self._queue_name = method_frame.method.queue
        LOGGER.info('Binding exchange %s to %s with routing %s, headers %s', 
                    self._exchange_name, self._queue_name, self._routing_key, self._headers)
        cb = functools.partial(self.on_bindok, userdata=self._queue_name)
        self._channel.queue_bind(
            self._queue_name,
            self._exchange_name,
            routing_key=self._routing_key,
            arguments=self._headers,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue_name, self.on_message, auto_ack=True)
        self._consuming = True

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def get_channel(self):
        return self._channel