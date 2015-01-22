import time
import socket
import msgpack
import sys

UDP_IP = "127.0.0.1"
UDP_PORT = 7955

NS = 'test.abc'
TS = None
DATA = 'Hello, World'
DOCUMENTATION_URL = 'http://example.com/'

msg_dict = {
    'ns': NS,
    'ts': TS,
    'data': DATA,
    'documentation_url': DOCUMENTATION_URL,
    'type': 'thing_update'
}

msg_list = [NS, TS, DATA, DOCUMENTATION_URL]

msg = msg_dict

if len(sys.argv) > 1:
    if sys.argv[1] == 'list':
        msg = msg_list


MESSAGE = msgpack.dumps(msg)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
