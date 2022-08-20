

# 1. connect to agent vm queue and inform endpoint name
# 2. start consume queue which is corresponds with endpoint name
import akross
import asyncio
import aiohttp
import aio_pika
import aiohttp
import json
from aio_pika.abc import AbstractIncomingMessage
from aio_pika import Message, connect, ExchangeType
import time
import logging
from akross.providers.rpc import AsyncStreamProvider



URL = 'https://api.upbit.com'
PROVIDER = 'upbit.broker.crypto.default.stream.default'
TRADE = 'trade'


class UpbitStreamProvider(AsyncStreamProvider):
    def __init__(self, amqp_url):
        super().__init__(PROVIDER,
                         amqp_url,
                         200)
        self.rprice = self.on_rprice
        self._exchanges = {}

    async def start_stream(self, symbol):
        session = aiohttp.ClientSession()
        print('START STREAM', str(symbol))

        new_channel = await self.connection.channel()
        
        self._exchanges[symbol] = await new_channel.declare_exchange(
            name= symbol + '.' + self.uuid, type=ExchangeType.FANOUT, auto_delete=True)

        async with session.ws_connect('wss://api.upbit.com/websocket/v1') as ws:
            req = [{"ticket": TRADE}, {"type": TRADE, "codes": [symbol]}]
            print('req', req)
            
            await ws.send_str(str(req).replace('\'', '\"'))
            async for msg in ws:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data == 'close':
                        await ws.close()
                        break
                    else:
                        print('receive ', msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    print('send stream')
                    try:
                        await self._exchanges[symbol].publish(Message(msg.data), routing_key='', )
                    except Exception as e:
                        print(e)
                        await ws.close()
                        del self._exchanges[symbol]
                        break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        print('STREAM DONE')

    async def on_rprice(self, **kwargs):
        if 'symbol' not in kwargs:
            raise akross.SymbolError()

        symbol = kwargs['symbol']
        if symbol not in self._exchanges:
            asyncio.create_task(self.start_stream(symbol))
        return {'exchange': symbol + '.' + self.uuid}


async def main() -> None:
    loop = asyncio.get_running_loop()
    stream_provider = UpbitStreamProvider("amqp://192.168.10.70:5672")
    await stream_provider.run()
    await loop.run_forever()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
    LOGGER = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())