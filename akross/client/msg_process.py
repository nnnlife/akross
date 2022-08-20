import asyncio
import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import uuid
import json
import logging
import akross


LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')


class MsgProcess(object):
    def __init__(self, pika_url):
        self._is_connected = False
        self._connection = None
        self._queue = None
        self._channel = None
        self._subscribe_queue = None
        self._agent_exchange = None
        self.providers = []
        self._pika_url = pika_url
        self._msg_dict = dict()
        self._last_subscribed_msg = ''
        self._subscirbe_msg_total_count = 0

    @property
    def last_subscribed_msg(self):
        return self._last_subscribed_msg

    async def connect(self):
        LOGGER.info('Connecting to %s', self._pika_url)

        self._connection = await aio_pika.connect(self._pika_url)
        self._channel = await self._connection.channel()
        self._queue = await self._channel.declare_queue(
            name='', exclusive=True, auto_delete=True)

        self._subscribe_queue = await self._channel.declare_queue(
            name='', exclusive=True, auto_delete=True)

        try:
            self._agent_exchange = await self._channel.declare_exchange(
                name='agent',
                type=ExchangeType.HEADERS,
                auto_delete=True,
                passive=True)
        except aio_pika.exceptions.ChannelClosed as e:
            raise e
        
        await self._queue.consume(self._on_response_msg, no_ack=True)
        await self._subscribe_queue.consume(
            self._on_subscribe_message, no_ack=True)

        await self._get_providers()

    async def _on_response_msg(self, message: AbstractIncomingMessage) -> None:
        LOGGER.debug('response msg: %s', message)

        try:
            if len(message.correlation_id) > 0:
                if message.correlation_id in self._msg_dict:
                    self._msg_dict[message.correlation_id].set_result(message)
                else:
                    print(self._msg_dict, message.correlation_id)
                    LOGGER.warning('cannot find correaltion id: %s', message.correlation_id)
            else:
                data = json.loads(message.body)
        except:
            print("Message body is: %r" % message.body)


    async def _on_subscribe_message(self, message: AbstractIncomingMessage) -> None:
        LOGGER.info('Get Provider from agent : %s', str(message))
        self._last_subscribed_msg = message
        self._subscirbe_msg_total_count += 1

    async def _get_providers(self):
        LOGGER.info('Get Provider from agent')
        cid = str(uuid.uuid4())
        msg = aio_pika.Message(
            reply_to=self._queue.name,
            headers={
                'broadcast': True,
                'target': 'agent',
                'method': 'list'},
            body=b'',
            correlation_id=cid)

        res = await self._send_block_msg(
            self._agent_exchange, msg, '', cid)

        try:
            data = json.loads(res.body)
            LOGGER.warning('PROVIDERS: %s', data)
            self.providers = data
        except:
            LOGGER.warning('cannot parse provider msg: %s', data)
            self.providers = []

    async def _send_block_msg(self, target, msg, routing_key='', msg_id=''):
        LOGGER.debug('send request target(%s), routing_key(%s) msg_id(%s)',
            target, routing_key, msg_id)
        fut = asyncio.Future()
        self._msg_dict[msg_id] = fut
        print(self._msg_dict)
        await target.publish(msg, routing_key=routing_key)
        res = await fut
        return res

    async def get_providers(self):
        return self.providers

    async def _check_exchange(self, exchange_name):
        try_count = 100
        while try_count > 0:
            channel = await self._connection.channel()
            print('Check exchange', exchange_name)
            try:
                await channel.declare_exchange(name=exchange_name,
                                                    type=ExchangeType.FANOUT,
                                                    auto_delete=True,
                                                    passive=True)
            except Exception as e:
                print('try again', e)
                await asyncio.sleep(1)
                try_count -= 1
                continue
            
            break

    async def send_msg(self, provider, cmd, **kwargs):
        if cmd not in akross.commands:
            return None

        msg_id = str(uuid.uuid4())
        if akross.commands[cmd].cmd_type == akross.TO_PROVIDER_DIRECT_REQ:
            msg = aio_pika.Message(reply_to=self._queue.name,
                                   headers={'method': cmd},
                                    body=json.dumps(kwargs).encode(),
                                   correlation_id=msg_id)

            return await self._send_block_msg(self._channel.default_exchange,
                                              msg, provider, msg_id)
        elif akross.commands[cmd].cmd_type == akross.TO_AGENT_SUBSCRIBE:
            print('AGENT SUBSCRIBE MSG')
            provider_name = provider.split('.')[0]
            msg = aio_pika.Message(reply_to=self._queue.name,
                                    headers={'method': cmd, 'name': provider_name + '_broker', 'target': 'broker'},
                                    body=json.dumps(kwargs).encode(),
                                    correlation_id=msg_id)
            print('msg', msg)
            res = await self._send_block_msg(self._agent_exchange,
                                              msg, '', msg_id)
            if len(res.body) == 0:
                print('No available provider')
            else:
                try:
                    print('receive msg')
                    exchange_info = json.loads(res.body)
                
                    if (isinstance(exchange_info, dict) and
                        'result' in exchange_info and
                        exchange_info['result'] == 'ok' and
                        'msg' in exchange_info and
                        'exchange' in exchange_info['msg']):
                        exchange_name = exchange_info['msg']['exchange']
                        await self._check_exchange(exchange_name)
                        
                        print('bind to', exchange_name)
                        await self._subscribe_queue.bind(exchange_name)
                except Exception as e:
                    print(e)
            return res

        

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    msgp = MsgProcess('amqp://192.168.10.70:5672')
    asyncio.run(msgp.connect())
