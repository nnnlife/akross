
import asyncio
import akross
import requests
import logging
from akross.providers.rpc import AsyncRestProvider

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
               '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


URL = 'https://api.upbit.com'
PROVIDER = 'upbit.broker.crypto.default.rest.default'
minute_url = URL + '/v1/candles/minutes/'
day_url = URL + '/v1/candles/days'


class UpbitRestProvider(AsyncRestProvider):
    def __init__(self, amqp_url):
        super().__init__(PROVIDER,
                         amqp_url)
        self.minutes = self.on_minutes
        self.days = self.on_days

    async def on_minutes(self, **kwargs):
        if 'symbol' not in kwargs:
            raise akross.SymbolError()
        
        symbol = kwargs['symbol']
        interval = 1
        count = 10
        query = {'market': symbol, 'count': str(count)}
        res = requests.request('GET',
                            minute_url + str(interval),
                            params=query)
        return eval(res.text)

    async def on_days(self, **kwargs):
        if 'symbol' not in kwargs:
            raise akross.SymbolError()
        
        symbol = kwargs['symbol']
        count = 10
        query = {'market': symbol, 'count': str(count)}
        res = requests.request('GET', day_url, params=query)
        return eval(res.text)


async def main() -> None:
    loop = asyncio.get_running_loop()
    rest_provider = UpbitRestProvider("amqp://192.168.10.70:5672")
    await rest_provider.run()
    await loop.run_forever()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
    LOGGER = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())