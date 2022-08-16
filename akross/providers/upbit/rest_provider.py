
import asyncio
import aio_pika
import uuid
import json
from aio_pika.abc import AbstractIncomingMessage
from aio_pika import connect, ExchangeType
import requests
import time
import logging
import akross

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
               '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


URL = 'https://api.upbit.com'
PROVIDER = 'upbit.crypto.default.rest.default'
minute_url = URL + '/v1/candles/minutes/'
day_url = URL + '/v1/candles/days'

channel = None
vm_id = str(uuid.uuid4())

def get_minute_quote(symbol, interval, count):
    query = {'market': symbol, 'count': str(count)}
    res = requests.request('GET',
                     minute_url + str(interval),
                     params=query)
    return eval(res.text)


def get_day_quote(symbol, count):
    query = {'market': symbol, 'count': str(count)}
    res = requests.request('GET', day_url, params=query)
    return eval(res.text)


async def on_message(message: AbstractIncomingMessage) -> None:
    print(" [x] Received message %r" % message)
    print("Message body is: %r" % message.body)
    
    headers = message.headers
    res = None
    if 'method' in headers:
        if headers['method'] == akross.int_to_command(akross.MINUTES):
            res = get_minute_quote('KRW-BTC', 30, 10)
        elif headers['method'] == akross.int_to_command(akross.DAYS):
            res = get_day_quote('KRW-BTC', 30)
    if res:
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(res).encode(), correlation_id=message.correlation_id),
            routing_key=message.reply_to)
    # ack is required to prevent get request before it is processed
    await message.ack()


async def send_health_check(exchange):
    while True:
        print('HEALTH Check')
        try:
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(
                        {'provider':PROVIDER, 
                        'uuid': vm_id,
                        'type': 'rest',
                        'time': akross.get_msec(),
                        'subscirbe': [],
                        'capacity': 0}
                    ).encode(),
                    headers={'name': 'upbit_broker', 'target': 'agent', 'method': 'regist'}
                ),
                routing_key=''
            )
        except aio_pika.ChannelNotFoundEntity:
            pass #TODO
        await asyncio.sleep(10)


async def main() -> None:
    global channel
    connection = await connect("amqp://192.168.10.70:5672")

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(name=PROVIDER, exclusive=False, auto_delete=True)
        await queue.consume(on_message)


        try:
            agent_exchange = await channel.declare_exchange(name='agent',
                                                            type=ExchangeType.HEADERS,
                                                            auto_delete=False,
                                                            passive=True)

            system_exchange = await channel.declare_exchange(name='system',
                                                             type=ExchangeType.TOPIC,
                                                             auto_delete=False,
                                                             passive=True)
        except aio_pika.exceptions.ChannelClosed as e:
            print('Exchange not exist', e)
            return None
        
        asyncio.create_task(send_health_check(agent_exchange))
        await asyncio.Future()


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
    LOGGER = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    print(" [*] Waiting for messages. To exit press CTRL+C")

    asyncio.run(main())