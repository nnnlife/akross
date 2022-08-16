import clientqueue
from pika.exchange_type import ExchangeType
import logging

LOGGER = logging.getLogger(__name__)


class System(clientqueue.ClientQueue):
    def __init__(self,
                 agent_name,
                 agent_type,
                 connection,
                 provider_outlet):
        super().__init__(connection,
                         None,
                         exchange_name='system',
                         exchange_type=ExchangeType.topic)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
                    

                
