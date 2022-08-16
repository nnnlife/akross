import time
from PyQt5.QtCore import QCoreApplication
from PyQt5 import QtCore
import pika
from pika.exchange_type import ExchangeType
from pika.spec import BasicProperties
import uuid
import time
import json
from pikaqt import PikaQtLoop
import stock_chart
from datetime import datetime

from akross.providers.cybos.stock_subscribe import StockSubscribe
import functools
import logging

from cybosconnection import CybosConnection


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

PROVIDER='cybos.stock.krx.stream.default'


class RabbitMQManager(QtCore.QObject):
    messageReceived = QtCore.pyqtSignal(bytes)

    def __init__(self):
        self.channel = None
        self._connection = pika.SelectConnection(
            pika.ConnectionParameters(host='192.168.10.70'),
            on_open_callback=self.on_connection_open, custom_ioloop=PikaQtLoop())
        self.subscribe_table = dict()

    def on_connection_open(self, _unused_connection):
        print('on_connection_open')
        self._connection.channel(on_open_callback=self.on_channel_open)
        
    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.queue_declare(queue='',
                                   exclusive=False,
                                   auto_delete=True,
                                   callback=self.on_queue_declareok)

    def on_queue_declareok(self, method_frame):
        self.request_queue = method_frame.method.queue
        self.channel.basic_consume(
            queue=self.request_queue,
            on_message_callback=self.on_response,
            auto_ack=False
        )
        self.regist()

    def regist(self):
        print('regist')
        #ms = int(time.time_ns() / 1000000)
        self.channel.basic_publish(exchange='agent',
                                routing_key='',
                                properties=BasicProperties(headers={
                                                                    'name': 'cybos_broker',
                                                                    'target': 'agent',
                                                                    'method': 'regist'}),
                                body=json.dumps({'provider': PROVIDER,
                                                    'uuid': self.request_queue,
                                                    'type': 'stream',
                                                    'time': 1000000,
                                                    'subscribe': [],
                                                    'capacity': 400}).encode()
                                )
        print('regist done')

    def stock_data_arrived(self, code, data_arr):
        self.channel.basic_publish(exchange=code + '.' + self.request_queue,
                                   routing_key='',
                                   body=json.dumps(data_arr).encode())        

    def on_exchange_declareok(self, _unused_frame, userdata, props, delivery_tag):
        self.channel.basic_publish(exchange='',
                                   routing_key=props.reply_to,
                                   properties=props,
                                   body=json.dumps({'exchange': userdata + '.' + self.request_queue}).encode())
        self.subscribe_table[userdata] = StockSubscribe(userdata, self.stock_data_arrived)
        self.subscribe_table[userdata].start_subscribe()
        self.channel.basic_ack(delivery_tag)

    def on_response(self, ch, method, props, body):
        print('Message Received')

        tmp_name = 'A005930'
        cb = functools.partial(self.on_exchange_declareok,
                               userdata=tmp_name,
                               props=props,
                               delivery_tag = method.delivery_tag)
        self.channel.exchange_declare(
            exchange=tmp_name + '.' + self.request_queue,
            exchange_type=ExchangeType.fanout,
            auto_delete=True, #TODO: True???
            callback=cb)


def run():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    app = QCoreApplication([])
    import signal
    import sys

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    conn = CybosConnection()
    if conn.is_connected():
        rmq = RabbitMQManager()

    sys.exit(app.exec_())


if __name__ == '__main__':
    run()
