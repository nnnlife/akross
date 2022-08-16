import asyncio
from aio_pika.abc import AbstractIncomingMessage
from aio_pika import connect, ExchangeType
import sys
import functools
from msg_process import MsgProcess
import json
import akross


msg_process = None


class Prompt:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.q = asyncio.Queue()
        self.loop.add_reader(sys.stdin, self.got_input)

    def got_input(self):
        asyncio.ensure_future(self.q.put(sys.stdin.readline()), loop=self.loop)

    async def __call__(self, msg, end='\n', flush=False):
        print(msg, end=end, flush=flush)
        return (await self.q.get()).rstrip('\n')


prompt = Prompt()
raw_input = functools.partial(prompt, end='', flush=True)


CMD_ROOT = 0
CMD_PROGRESS = 1
CMD_COMPLETE = 2
GO_UP = 'up'
GO_ROOT = 'root'
GET_LAST_MSG = 'last'
GET_HINT = 'hint'

async def auto_complete(current_level, command, current=''):
    providers = await msg_process.get_providers()
    search = current + ('.' if len(current) else '') + command
    candidates = []
    for provider in providers:
        print(provider['provider'], search)
        if provider['provider'].startswith(search):
            candidates.append(provider)

    if len(candidates) == 1:
        return CMD_COMPLETE, candidates[0]['provider'] # complete
    elif len(candidates) > 1:
        level_arr = []
        for candidate in candidates:
            level_arr.append(candidate['provider'].split('.'))
        
        for i, key in enumerate(level_arr[0]):
            for other in level_arr[1:]:
                if len(other) < i + 1 or other[i] != key:
                    return CMD_PROGRESS, '.'.join(level_arr[0][:i])

    return current_level, current
    

# TODO: do something automatically to search input based on registered commands
async def get_input():
    current_level = CMD_ROOT
    prefix = 'root'
    while True:
        command = (await raw_input(prefix + '> ')).strip()
        commands = command.split(' ')
        args = []
        if len(commands) > 1:
            command = commands[0]
            args = commands[1:]

        if command == GO_UP or command == GO_ROOT:
            if command == GO_ROOT or '.' not in prefix:
                prefix = 'root'
                current_level = CMD_ROOT
            else:
                prefix = '.'.join(prefix.split('.')[:-1])
                current_level = CMD_PROGRESS
        elif command == GET_LAST_MSG:
            print(msg_process.last_subscribed_msg)
        else:                
            if current_level == CMD_ROOT:
                current_level, prefix = await auto_complete(current_level, command)
                if len(prefix) == 0:
                    prefix = 'root'
            elif current_level == CMD_PROGRESS:
                current_level, prefix = await auto_complete(current_level, command, prefix)
            elif current_level == CMD_COMPLETE:
                if command == GET_HINT: #TODO: decision point whether provider send hint or use constant table
                    pass
                else:
                    res = await msg_process.send_msg(prefix, command, *args)
                    if res:
                            print('RESPONSE', json.loads(res.body))
                    else:
                        print('Cannot find command', command)
                    

async def main() -> None:
    global msg_process
    msg_process = MsgProcess('amqp://192.168.10.70:5672')
    await msg_process.connect()
    await get_input()


if __name__ == '__main__':
    import logging
    # LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
    #             '-35s %(lineno) -5d: %(message)s')
    # LOGGER = logging.getLogger(__name__)
    # logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()