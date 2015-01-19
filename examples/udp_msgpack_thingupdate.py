import time
import socket
import msgpack

UDP_IP = "127.0.0.1"
UDP_PORT = 7955

msg = {
    'ns': 'test.abc',
    'data': "Hello, World!",
    'documentation_url': 'http://example.com/',
    'type': 'thing_update'
}

MESSAGE = msgpack.dumps(msg)

sock = socket.socket(socket.AF_INET,  # Internet
                     socket.SOCK_DGRAM)  # UDP
sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
