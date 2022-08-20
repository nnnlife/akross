import json
import asyncio
import aio_pika
import uuid
import akross
from aio_pika.abc import AbstractIncomingMessage


class Rpc(object):
    def __init__(self, provider_endpoint, _url):
        self._request_handlers = {}
        self._provider_endpoint = provider_endpoint
        self._provider_name = provider_endpoint.split('.')[0]
        self._provider_type = provider_endpoint.split('.')[1]
        self._service_type = provider_endpoint.split('.')[4]
        self._url = _url
        self._uuid = str(uuid.uuid4())
        self._subscribe_list = []
        self._capacity = 0

    def __register(self, method_name, callback):
        self._request_handlers[method_name] = callback

    def __setattr__(self, method_name, callback):
        if method_name.startswith("_"):  # prevent calls for private methods
            return super().__setattr__(method_name, callback)
        return self.__register(method_name, callback)

    @property
    def capacity(self):
        return self._capacity

    @property
    def request_handler(self):
        return self._request_handlers

    @property
    def subscribe_list(self):
        return self._subscribe_list

    @property
    def provider_endpoint(self):
        return self._provider_endpoint

    @property
    def provider_name(self):
        return self._provider_name

    @property
    def provider_type(self):
        return self._provider_type

    @property
    def uuid(self):
        return self._uuid

    @property
    def service_type(self):
        return self._service_type


class StreamRequest(Rpc):
    pass


class RestRequest(Rpc):
    def __init__(self,
                 headers,
                 body):
        super().__init__(headers, body)


class AsyncProvider(Rpc):
    def __init__(self, _provider_name, _url):
        super().__init__(_provider_name, _url)

    async def _connect(self):
        self._connection = await aio_pika.connect(self._url)
        return self._connection

    @property
    def connection(self):
        return self._connection

    async def _setup_system_msg(self):
        self._system_queue = await self._channel.declare_queue(
            name='', exclusive=True, auto_delete=True)

    async def _send_health_check(self, exchange):
        while True:
            try:
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(
                            {'provider':self.provider_endpoint, 
                            'uuid': self.uuid,
                            'service': self.service_type,
                            'time': akross.get_msec(),
                            'subscribe': self.subscribe_list,
                            'capacity': self.capacity}
                        ).encode(),
                        headers={'name': self.provider_name + '_' + self.provider_type,
                                 'target': 'agent',
                                 'method': 'regist'}
                    ),
                    routing_key=''
                )
            except aio_pika.ChannelNotFoundEntity:
                pass #TODO
            await asyncio.sleep(10)

    async def _check_exchange(self, conn):
        while True:
            agent_exchange = None
            system_exchange = None
            channel = await conn.channel()
            try:
                agent_exchange = await channel.declare_exchange(name='agent',
                                                                type=aio_pika.ExchangeType.HEADERS,
                                                                auto_delete=False,
                                                                passive=True)

                system_exchange = await channel.declare_exchange(name='system',
                                                                type=aio_pika.ExchangeType.TOPIC,
                                                                auto_delete=False,
                                                             passive=True)
            except aio_pika.exceptions.ChannelClosed as e:
                await asyncio.sleep(3)
                print('Try again...')

            self._channel = channel
            self._agent_exchange = agent_exchange
            self._system_exchange = system_exchange
            print('Done')
            break

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        headers = message.headers
        res = {'result': 'error', 'msg': ''}
        
        print('on_message', message)
        if 'method' in headers:
            if headers['method'] in self.request_handler:
                try:
                    body = json.loads(message.body)
                    if isinstance(body, dict):
                        try:
                            result = await self.request_handler[headers['method']](**body)
                            res = {'result': 'ok', 'msg': result}
                        except akross.AkrossException as e:
                            res['msg'] = str(e)
                    else:
                        res['msg'] = 'body is not dict instance'
                except:
                    res['msg'] = 'body is not json type'
            else:
                res['msg'] = 'method not implemented'
        else:
            res['msg'] = 'wrong message format'

        await self._channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(res).encode(), correlation_id=message.correlation_id),
            routing_key=message.reply_to)
        await message.ack()

    async def run(self):
        await self._setup()
        if self._agent_exchange:
            await asyncio.create_task(self._send_health_check(self._agent_exchange))
            print('setup done')
        else:
            print('setup is not done')       


class AsyncStreamProvider(AsyncProvider):
    def __init__(self, _provider_name, _url, capacity):
        super().__init__(_provider_name, _url)
        self._capacity = capacity
    
    async def _setup(self):
        conn = await self._connect()
        await self._check_exchange(conn)
        
        self._queue = await self._channel.declare_queue(
            name='', exclusive=True, auto_delete=True)
        self._uuid = self._queue.name
        await self._queue.consume(self.on_message)

        await self._setup_system_msg()


class AsyncRestProvider(AsyncProvider):
    def __init__(self, _provider_name, _url):
        super().__init__(_provider_name, _url)

    async def _setup(self):
        conn = await self._connect()
        await self._check_exchange(conn)
        self._queue = await self._channel.declare_queue(
            name=self.provider_endpoint, exclusive=False, auto_delete=True)        
        await self._queue.consume(self.on_message)
        await self._setup_system_msg()



