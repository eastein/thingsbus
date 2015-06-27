from __future__ import absolute_import
from __future__ import print_function
import thingsbus.service_discovery as sd
from zmqfan import zmqsub
import threading
import six.moves.queue
import time
from . import thing
import pprint

class Adaptor(threading.Thread):

    def __init__(self, ns, documentation_url, broker_input_url=None, zone=None, verbose=False):

        if (zone is None) and (broker_input_url is None):
            raise RuntimeError('Must supply zone or broker_input_url')
        if not ns:
            raise RuntimeError('ns must be set.')

        self.verbose = verbose
        self.zone = zone
        self._broker_input_url = broker_input_url

        self.ns = ns
        self.ns_parts = thing.parse_ns(ns)
        self.documentation_url = documentation_url

        self.ok = True
        self.msg_q = six.moves.queue.Queue()

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
        if self.verbose:
            print('%s connecting to %s' % (self, url))
        self.broker_input = zmqsub.ConnectPub(url)

        while self.ok:
            try:
                # TODO use polling instead of assuming we can send
                msg_tosend = self.msg_q.get(timeout=0.05)
                if self.verbose:
                    print('%s sending message...' % self)
                    pprint.pprint(msg_tosend)
                self.broker_input.send(msg_tosend)
            except six.moves.queue.Empty:
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
