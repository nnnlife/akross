import logging
import time
import pika
from akross.agent.provider_outlet import ProviderOutlet


LOGGER = logging.getLogger(__name__)


class _Connection(object):
    def __init__(self, amqp_url):
        self.should_reconnect = False
        self._consuming = False
        self.was_consuming = False

        self._connection = None
        self._closing = False
        self._url = amqp_url
        self._channels = []

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def get_connection(self):
        return self._connection

    def close_connection(self):
        for ch in self._channels:
            if not ch.is_connection_closed:
                return

        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def open_channel(self):
        for ch in self._channels:
            ch.open_channel()
        self.was_consuming = True

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def stop_consuming(self):
        for ch in self._channels:
            ch.stop_consuming()

    def add_channel(self, channel):
        self._channels.append(channel)

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')


class Connection(object):
    def __init__(self, amqp_url, agent_name, agent_type):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._connection = None
        self._agent_name = agent_name
        self._agent_type = agent_type
        self._CHANNEL_CLASSES = []
        self._provider_outlet = ProviderOutlet()

    def add_channel_class(self, channel):
        self._CHANNEL_CLASSES.append(channel)

    def _create_connection(self):
        self._connection = _Connection(self._amqp_url)
        for CLASS in self._CHANNEL_CLASSES:
            self._connection.add_channel(CLASS(self._agent_name,
                                               self._agent_type,
                                               self._connection,
                                               self._provider_outlet))

    def run(self):
        while True:
            try:
                self._create_connection()
                self._connection.run()
            except KeyboardInterrupt:
                self._connection.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._connection.should_reconnect:
            self._connection.stop()
            reconnect_delay = self._get_reconnect_delay()
            self._provider_outlet.clear_provider()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._create_connection()

    def _get_reconnect_delay(self):
        if self._connection.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
