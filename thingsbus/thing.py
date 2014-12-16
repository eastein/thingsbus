import time
import threading
import copy
import pprint


class InternalException(Exception):
    pass


class BadMessageException(Exception):
    pass


class Thing(object):

    """
    ns is a dot delimited namespace. The name of the Thing itself should appear last.
    """

    def __init__(self, ns):
        self.ns = ns
        self.data_lock = threading.Lock()
        self.last_data = None
        self.last_data_ts = None

    def set_data(self, data, ts):
        with self.data_lock:
            self.last_data = data
            self.last_data_ts = ts

    def get_data(self):
        now = time.time()
        with self.data_lock:
            if self.last_data_ts is None:
                return None

            return (now - self.last_data_ts), copy.deepcopy(self.last_data)

    def __repr__(self):
        return 'Thing<%s>' % self.ns

    def __str__(self):
        return repr(self)


class Directory(object):

    def __init__(self, thing_class=None):
        self.name_to_thing = dict()
        if thing_class is None:
            thing_class = thing.Thing
        self.thing_class = thing_class

    def get_thing(self, ns):
        if ns not in self.name_to_thing:
            self.name_to_thing[ns] = self.thing_class(ns)

        return self.name_to_thing[ns]

    def handle_data_set(self, msg):
        for k in ['ns', 'data']:
            if k not in msg:
                raise BadMessageException("%s not in message." % k)

        # TODO validate ns, ts - data really can be anything.
        ns = msg['ns']
        ts = msg.get('ts', time.time())

        data = msg['data']

        # evidently we'll overwrite our own module if we use the same identifier. Fun! Do not do that.
        _thing = self.get_thing(ns)
        _thing.set_data(data, ts)

        return {
            'type': 'thing_update',
            'ns': ns,
            'data': data,
            'ts': ts
        }

    def handle_message(self, msg, accept_snapshots=False):
        if type(msg) != dict:
            raise BadmessageException("Dictionaries are the only valid top level message.")

        if 'type' not in msg:
            raise BadMessageException("Messages must be typed.")

        if msg['type'] == 'thing_update':
            return self.handle_data_set(msg)
        elif msg['type'] == 'thing_snapshot':
            if accept_snapshots:
                if 'data' not in msg:
                    raise BadMessageException("Snapshot must have a data key.")
                if type(msg['data']) != dict:
                    raise BadMessageException("Snapshot data must be a dictionary.")

                for data_value in msg['data'].values():
                    self.handle_data_set(data_value)
        else:
            raise BadmessageException("Don't know how to handle message of type %s" % msg['type'])
