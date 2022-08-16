

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



LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
               '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


URL = 'https://api.upbit.com'
PROVIDER = 'upbit.crypto.default.stream.default'
TRADE = 'trade'
exchange_set = dict()

connection = None
channel = None
queue_name = ''


async def start_stream(symbol):
    global exchange_set
    session = aiohttp.ClientSession()
    print('START STREAM', str(symbol))

    new_channel = await connection.channel()
    
    exchange_set[symbol] = await new_channel.declare_exchange(
        name= symbol + '.' + queue_name, type=ExchangeType.FANOUT, auto_delete=True)

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
                    await exchange_set[symbol].publish(Message(msg.data), routing_key='', )
                except Exception as e:
                    await ws.close()
                    del exchange_set[symbol]
                    break
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    print('STREAM DONE')

async def on_message(message: AbstractIncomingMessage) -> None:
    print(" [x] Received message %r" % message)
    print("Message body is: %r" % message.body)
    
    headers = message.headers

    if 'method' in headers:
        if headers['method'] == akross.int_to_command(akross.REALTIME_PRICE):
            try:
                symbol = json.loads(message.body)
                if isinstance(symbol, list) and len(symbol) > 0:
                    symbol = symbol[0]
                    if symbol not in exchange_set:
                        asyncio.create_task(start_stream(symbol))

                    await channel.default_exchange.publish(
                        aio_pika.Message(body=json.dumps({'exchange': symbol + '.' + queue_name}).encode(),
                        correlation_id=message.correlation_id),
                        routing_key=message.reply_to)
                    
            except:
                pass
    
    # ack is required to prevent get request before it is processed

    # apply correlation id
    
    await message.ack()


async def send_health_check(exchange):
    while True:
        print('HEALTH Check')
        # try:
        await exchange.publish(
            aio_pika.Message(
                body=json.dumps(
                    {'provider':PROVIDER, 
                    'uuid': queue_name,
                    'type': 'stream',
                    'capacity': 400,
                    'time': akross.get_msec(),
                    'subscribe': list(exchange_set),
                    'capacity': 200}
                ).encode(),
                headers={'name': 'upbit_broker', 'target': 'agent', 'method': 'regist'}
            ),
            routing_key='')
        # except Exception as e:
        #     print(e)
        #     break
            
        await asyncio.sleep(10)


async def main() -> None:
    global channel, queue_name, connection
    connection = await connect("amqp://192.168.10.70:5672")

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue('', exclusive=False, auto_delete=True)
        await queue.consume(on_message)
        queue_name = queue.name

        ms = int(time.time_ns() / 1000000)

        try:
            agent_exchange = await channel.declare_exchange(name='agent',
                                                            type=ExchangeType.HEADERS,
                                                            auto_delete=True)
                                                            # passive=True)            
        except aio_pika.exceptions.ChannelClosed as e:
            print('Exchange not exist', e)
            return None

        await asyncio.create_task(send_health_check(agent_exchange))


if __name__ == '__main__':
    asyncio.run(main())