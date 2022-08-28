
import asyncio
import uuid
import requests
import logging
from akross.providers.rpc import AsyncRestProvider

from binance import AsyncClient


PROVIDER = 'binance.broker.crypto.default.spot.rest'


class BinanceRestProvider(AsyncRestProvider):
    def __init__(self, amqp_url):
        super().__init__(PROVIDER,
                         amqp_url)
        self.minutes = self.on_minutes
        self.days = self.on_days
    
    @classmethod
    async def create(cls, amqp_url):
        bn = BinanceRestProvider(amqp_url)
        bn._client = await AsyncClient.create()
        return bn

    async def on_minutes(self, **kwargs):
        if 'symbol' not in kwargs:
            return await self._handle_argument_error('cannot find symbol')
        symbol = kwargs['symbol']
        result = await self._client.get_klines(
            symbol=symbol, interval=AsyncClient.KLINE_INTERVAL_1MINUTE)
        return result

    async def on_days(self, **kwargs):
        if 'symbol' not in kwargs:
            return await self._handle_argument_error('cannot find symbol')
        symbol = kwargs['symbol']
        result = await self._client.get_klines(
            symbol=symbol, interval=AsyncClient.KLINE_INTERVAL_1DAY)
        return result
        

async def main() -> None:
    loop = asyncio.get_running_loop()
    rest_provider = await BinanceRestProvider.create("amqp://192.168.10.70:5672")
    await rest_provider.run()
    await loop.run_forever()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
    LOGGER = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())