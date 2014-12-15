import thingsbus.service_discovery as sd
from zmqfan import zmqsub
import threading
import Queue
import time


class Adaptor(threading.Thread):

    def __init__(self, base_ns, broker_input_url=None, zone=None):
        threading.Thread.__init__(self)

        if (zone is None) and (broker_input_url is None):
            raise RuntimeError('Must supply zone or broker_input_url')
        if not base_ns:
            raise RuntimeError('Base ns must exist.')

        self.zone = zone
        self._broker_input_url = broker_input_url

        self.base_ns = base_ns
        self.ok = True
        self.msg_q = Queue.Queue()

    def stop(self):
        self.ok = False

    @property
    def broker_input_url(self):
        if self._broker_input_url is not None:
            return self._broker_input_url
        else:
            return sd.ServiceFinder.broker_input_url(self.zone)

    def run(self):
        url = self.broker_input_url
        self.broker_input = zmqsub.ConnectPub(url)

        while self.ok:
            try:
                # TODO use polling instead of assuming we can send
                self.broker_input.send(self.msg_q.get(timeout=0.05))
            except Queue.Empty:
                pass

    def send(self, data, ns=None, ts=None):
        if ts is None:
            ts = time.time()

        final_ns = ('%s.%s' % (self.base_ns, ns)).lower()

        msg = {
            'type': 'thing_update',
            'ns': final_ns,
            'data': data,
            'ts': ts
        }
        self.msg_q.put(msg)
