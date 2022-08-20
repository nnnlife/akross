import json

UNKNOWN = 0
# Commands

REGIST          = 1
PROVIDER_LIST   = 2
MINUTES         = 3
DAYS            = 4
REALTIME_PRICE  = 5

# Message Communication Type
# Default source is client, otherwise put source at front
TO_AGENT_BROADCAST          = 1
AGENT_TO_CLIENT_RESPONSE    = 2
TO_PROVIDER_DIRECT_REQ      = 3
TO_AGENT_SUBSCRIBE          = 4


_commands = {
    # command: int(command), comm type, minimum arguments count
    'regist':   (REGIST,            TO_AGENT_BROADCAST,         0),
    'list':     (PROVIDER_LIST,     AGENT_TO_CLIENT_RESPONSE,   0),
    'minutes':  (MINUTES,           TO_PROVIDER_DIRECT_REQ,     1),
    'days':     (DAYS,              TO_PROVIDER_DIRECT_REQ,     1),
    'rprice':   (REALTIME_PRICE,    TO_AGENT_SUBSCRIBE,         1),
}


class Command:
    def __init__(self,
                 cmd_name,
                 cmd_type,
                 minimum_arg_count):
        self._cmd_name = cmd_name
        self._cmd_type = cmd_type
        self._minimum_arg_count = minimum_arg_count

    @property
    def cmd_name(self):
        return self._cmd_name
    
    @property
    def cmd_type(self):
        return self._cmd_type

    @property
    def minimum_arg_count(self):
        return self._minimum_arg_count

commands = {}
for k, v in _commands.items():
    commands[k] = Command(*v)


def get_protocol_type(k):
    if k in commands:
        return commands[k][1]

    return UNKNOWN


def int_to_command(i):
    for k, c in _commands.items():
        if c[0] == i:
            return k
    return ''







