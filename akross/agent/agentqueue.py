import clientqueue
from pika.exchange_type import ExchangeType
import logging
import json


LOGGER = logging.getLogger(__name__)


class AgentQueue(clientqueue.ClientQueue):
    AGENT = 'agent'
    BROKER = 'broker'

    def __init__(self,
                 agent_name,
                 agent_type,
                 connection,
                 provider_outlet):
        headers = {'x-match': 'any',
                   'type': agent_type,
                   'name': agent_name + '_' + agent_type,
                   'broadcast': True}
        super().__init__(connection,
                         provider_outlet,
                         exchange_name='agent',
                         exchange_type=ExchangeType.headers,
                         headers=headers)

    def on_message(self, _unused_channel, basic_deliver, properties, body):

        if 'target' in properties.headers:
            if properties.headers['target'] == AgentQueue.AGENT:
                res = self.provider_outlet.process_agent_message(properties.headers, body)
                if res:
                    self.get_channel().basic_publish(exchange='',
                                                     routing_key=properties.reply_to,
                                                     properties=properties,
                                                     body=res)
            elif (properties.headers['target'] == AgentQueue.BROKER and
                  'name' in properties.headers):
                LOGGER.info('Agent Received message # %s(%s) from %s: %s(reply:%s)',
                            basic_deliver.delivery_tag, 
                            properties.app_id,
                            properties.headers,
                            body,
                            properties.reply_to)
                provider = self.provider_outlet.get_stream_provider(properties.headers['name'], body)
                if provider:
                    print('publish to provider')
                    self.get_channel().basic_publish(exchange='',
                                                     routing_key=provider['uuid'],
                                                     properties=properties,
                                                     body=body)
                else:
                    print('reply to')
                    self.get_channel().basic_publish(exchange='',
                                                     routing_key=properties.reply_to,
                                                     properties=properties,
                                                     body=json.dumps([]).encode())
                    

                
