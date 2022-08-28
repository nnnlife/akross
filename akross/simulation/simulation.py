import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import json
import asyncio
import logging

from akross.client.msg_process import MsgProcess


LOGGER = logging.getLogger(__name__)


class Simulation:
    def __init__(self, pika_url):
        self._control = {
            'start': self._start,
            'stop': self._stop,
            'from': self._set_from,
            'until': self._set_until,
            'speed': self._set_speed,
            'pause': self._pause,
            'reset': self._reset,
            'target': self._set_target
        }
        self._pika_url = pika_url
    
    async def _start(self, **kwargs):
        raise NotImplementedError

    async def _stop(self, **kwargs):
        raise NotImplementedError

    async def _set_from(self, **kwargs):
        raise NotImplementedError
    
    async def _set_until(self, **kwargs):
        raise NotImplementedError

    async def _set_speed(self, **kwargs):
        raise NotImplementedError

    async def _pause(self, **kwargs):
        raise NotImplementedError

    async def _reset(self, **kwargs):
        raise NotImplementedError

    async def _set_target(self, **kwargs):
        raise NotImplementedError

    async def send_msg(self, msg):
        await self._simulation_exchange.publish(msg, routing_key='')

    async def connect(self):
        LOGGER.info('Connecting to %s', self._pika_url)
        self._connection = await aio_pika.connect(self._pika_url)
        self._channel = await self._connection.channel()
        self._queue = await self._channel.declare_queue(
            name='simulation', exclusive=True, auto_delete=True)
        try:
            self._simulation_exchange = await self._channel.declare_exchange(
                name='simulation_data',
                type=ExchangeType.FANOUT,
                auto_delete=True,
                passive=True)
        except aio_pika.exceptions.ChannelClosed as e:
            raise e

        await self._queue.consume(self._on_response_msg, no_ack=True)
    
    async def _on_response_msg(self, message: AbstractIncomingMessage) -> None:
        headers = message.headers
        if 'method' in headers:
            control = headers['method']
            arguments = {}
            try:
                arguments = json.loads(message.body)
            except:
                pass
            if control in self._control:
                self._control[control](**arguments)


class AverageAlign(Simulation):
    def __init__(self, pika_url):
        super().__init__(pika_url)
        self.msg_process = MsgProcess(pika_url)
        self.symbols = ['binance.crypto.spot.BTCUSDT']

    async def run(self):
        await self.connect()
        await self.msg_process.connect()

    async def _start(self, **kwargs):
        msg = aio_pika.Message(
            headers={'method': 'simulation_symbols'},
            body=json.dumps(self.symbols).encode())
        self.send_msg(msg)

    async def _stop(self, **kwargs):
        pass

    async def _set_from(self, **kwargs):
        pass
    
    async def _set_until(self, **kwargs):
        pass

    async def _set_speed(self, **kwargs):
        pass

    async def _pause(self, **kwargs):
        pass

    async def _reset(self, **kwargs):
        pass

    async def _set_target(self, **kwargs):
        pass


async def main():
    loop = asyncio.get_event_loop()

    simluation = AverageAlign('amqp://192.168.10.70:5672')
    await simluation.run()
    loop.run_forever()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    asyncio.run(main())
    
    