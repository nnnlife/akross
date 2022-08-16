import sys
import logging

from akross.agent.connection import Connection
from akross.agent.agentqueue import AgentQueue
from akross.agent.system import System


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    
    if len(sys.argv) < 3:
        print(f'Usage: python3 {sys.argv[0]} agent_name type')
        print('Example: python3 main.py upbit broker')
        sys.exit(1)

    print('Running')
    conn = Connection(
        'amqp://192.168.10.70:5672', sys.argv[1], sys.argv[2])
    conn.add_channel_class(AgentQueue)
    conn.add_channel_class(System)

    conn.run()