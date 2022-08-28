import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import json
import asyncio
import logging

"""
SYMBOL
TYPE: 이미 이름에서 결정 (stock, future, crypto..)
BROKER: 생략 이미 이름에서 결정
COUNTRY: 생략, 이름에서 KRX 등 결정, CRYPTO의 경우 broker 이름이 country
DESC (Trading View 경우 BITCOIN / TETHERUS)
Base Asset: 차후 Asset으로 연결
Quote Asset: 차후 Asset 으로 연결
Base Asset Precision
Quote Asset Precision
ICON
"""
LOGGER = logging.getLogger(__name__)


class SymbolService:
    def __init__(self, pika_url):
        self._pika_url = pika_url

    async def connect(self):
        LOGGER.info('Connecting to %s', self._pika_url)
        self._connection = await aio_pika.connect(self._pika_url)
        self._channel = await self._connection.channel()
        self._queue = await self._channel.declare_queue(
            name='symbol_service', exclusive=True, auto_delete=True)

        await self._queue.consume(self._on_response_msg, no_ack=True)
    
    async def handle_symbol_info(self, args, reply_to, correlation_id):
        infos = []
        infos.append({
            'symbol': 'binance.crypto.spot.BTCUSDT',
            'desc': 'BTC / THETHUR',
            'base_asset': 'BTC',
            'quote_asset': 'USDT',
            'base_precision': 8,
            'asset_precision': 8,
            'icon': ''})
        msg = aio_pika.Message(body=json.dumps(infos).encode())
        msg.correlation_id = correlation_id
        self._channel.default_exchange.publish(msg, routing_key=reply_to)

    async def _on_response_msg(self, message: AbstractIncomingMessage) -> None:
        headers = message.headers
        if 'method' in headers:
            if 'info' in headers['method']:
                try:
                    arguments = json.loads(message.body)
                    if isinstance(arguments, list):
                        await self.handle_symbol_info(
                            arguments, message.reply_to, message.correlation_id)
                except:
                    pass



async def main():
    loop = asyncio.get_event_loop()

    symbol_service = SymbolService('amqp://192.168.10.70:5672')
    await symbol_service.run()
    loop.run_forever()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    asyncio.run(main())
    