import logging
import json
from datetime import timedelta
import time
import akross


LOGGER = logging.getLogger(__name__)


class ProviderOutlet(object):
    UNKNOWN =   0
    REPLY =     1
    INTERNAL =  2

    def __init__(self):
        self.methods = {
            'regist':self._regist,
            'list':self._list
        }
        self.providers = {}

    def _regist(self, **kwargs):
        print('regist called')
        if 'uuid' in kwargs:
            if kwargs['uuid'] in self.providers:
                provider = self.providers[kwargs['uuid']]
                provider['time'] = int(time.time_ns() / 1000000)
            else:
                self.add_provider(kwargs)
        return None
        
    def _list(self, **kwargs):
        print('list called')
        return self.dumps()

    def add_provider(self, provider):
        """
            await exchange.publish(
            aio_pika.Message(
                body=json.dumps(
                    {'provider':PROVIDER, 
                     'uuid': vm_id,
                     'type': 'rest',
                     'time': ms,
                     'subscirbe': [],
                     'capacity': 0}
                ).encode(),
                headers={'name': 'upbit_broker', 'target': 'agent', 'method': 'regist'}
            ),
            routing_key=''
        """
        self.providers[provider['uuid']] = provider
        LOGGER.info('Add provider(%d): %s', len(self.providers), provider)

    def clear_provider(self):
        self.providers.clear()

    def _remove_timeout_provider(self):
        for k, provider in self.providers.items():
            if akross.msec_diff(provider['time']) > timedelta(seconds=15):
                del self.providers[k]
    def dumps(self):
        # 1. remove timeout nodes from providers
        self._remove_timeout_provider()

        # 2. remove duplicated same provider endpoint name
        provider_endpoint = []
        final_deliver = []
        for k, provider in self.providers.items():
            if provider['provider'] in provider_endpoint:
                continue
            else:
                provider_endpoint.append(provider['provider'])
                final_deliver.append(provider)
        print('final deliver', final_deliver)
        return json.dumps(final_deliver).encode()

    def process_agent_message(self, headers, body):
        print('process_agent_message', headers, body)
        try:
            content = json.loads(body) if len(body) > 0 else {}
            res = self.methods[headers['method']](**content)
        except Exception as e:
            print(e)
            res = None

        print('return', res)
        return res

    def get_stream_provider(self, name, body):
        self._remove_timeout_provider()
        providers = []
        for k, provider in self.providers.items():
            provider_name = name.split('_')[0]
            if provider['type'] == 'stream' and provider['provider'].startswith(provider_name):
                providers.append(provider)
        
        if len(providers) > 0:
            try:
                symbol = json.loads(body)
                if type(symbol) == isinstance(symbol, list) and len(symbol) > 0:
                    symbol = symbol[0]
                    for provider in self.providers:
                        if symbol in provider['subscribe']:
                            return provider
            except:
                pass

            providers = sorted(providers, key=lambda p: p['capacity'] - len(p['subscribe']))
            return providers[-1]

        return None


        