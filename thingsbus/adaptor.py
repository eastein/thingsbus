import thingsbus.service_discovery as sd
from zmqfan import zmqsub
import threading
import Queue
import time
import thing


class Adaptor(threading.Thread):

    def __init__(self, ns, documentation_url, broker_input_url=None, zone=None):

        if (zone is None) and (broker_input_url is None):
            raise RuntimeError('Must supply zone or broker_input_url')
        if not ns:
            raise RuntimeError('ns must be set.')

        self.zone = zone
        self._broker_input_url = broker_input_url

        self.ns = ns
        self.ns_parts = thing.parse_ns(ns)
        self.documentation_url = documentation_url

        self.ok = True
        self.msg_q = Queue.Queue()

        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    def stop(self):
        self.ok = False

    @property
    def broker_input_url(self):
        if self._broker_input_url is not None:
            return self._broker_input_url
        else:
            return sd.ServiceFinder.broker_input_url(self.zone)

    def __repr__(self):
        s = 'Adaptor<ns=%s' % self.ns
        if self._broker_input_url is not None:
            s += (', broker_input_url=' + self._broker_input_url)
        if self.zone is not None:
            s += (', zone=' + self.zone)
        s += '>'
        return s

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

        if ns is None:
            final_ns = self.ns
        else:
            final_ns = thing.stringify_ns(self.ns_parts + thing.parse_ns(ns))

        msg = {
            'type': 'thing_update',
            'ns': final_ns,
            'documentation_url': self.documentation_url,
            'data': data,
            'ts': ts
        }

        self.msg_q.put(msg)
