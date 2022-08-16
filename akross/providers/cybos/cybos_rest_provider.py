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

import logging
import win32com.client

from cybosconnection import CybosConnection


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

PROVIDER='cybos.stock.krx.rest.default'

vm_id = str(uuid.uuid4())


class RabbitMQManager(QtCore.QObject):
    messageReceived = QtCore.pyqtSignal(bytes)

    def __init__(self):
        self.channel = None
        self._connection = pika.SelectConnection(
            pika.ConnectionParameters(host='192.168.10.70'),
            on_open_callback=self.on_connection_open, custom_ioloop=PikaQtLoop())

    def on_connection_open(self, _unused_connection):
        print('on_connection_open')
        self._connection.channel(on_open_callback=self.on_channel_open)
        
    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.queue_declare(queue=PROVIDER,
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
                                                                    'name': 'upbit_broker',
                                                                    'target': 'agent',
                                                                    'method': 'regist'}),
                                body=json.dumps({'provider': PROVIDER,
                                                    'uuid': vm_id,
                                                    'type': 'rest',
                                                    'time': 1000000,
                                                    'subscribe': [],
                                                    'capacity': 0}).encode()
                                )
        print('regist done')

    def on_response(self, ch, method, props, body):
        print('Message Received')
        _, data = stock_chart.get_min_period_data('A005930', datetime(2022, 8, 8), datetime(2022, 8, 9))
        print('DATA', len(data))
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=props,
                         body=json.dumps(data).encode())
        ch.basic_ack(method.delivery_tag)


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
