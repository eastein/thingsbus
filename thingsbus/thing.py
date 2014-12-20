import time
import threading
import copy
import pprint
import re


class InternalException(Exception):
    pass


class BadMessageException(Exception):
    pass


class BadNamespaceException(Exception):
    pass


NS_PART_REGEXSTR = '([a-z]{1}[a-z0-9\-\_]{0,63})'
NS_PART_REGEX = re.compile('^%s$' % NS_PART_REGEXSTR)


def parse_ns(ns):
    ns_parts = []
    if ns != '':
        for ns_part in ns.split('.'):
            nsp_m = NS_PART_REGEX.match(ns_part)
            if nsp_m is None:
                raise BadNamespaceException()
            ns_parts.append(nsp_m.groups(0)[0])
    return ns_parts


def stringify_ns(nsl):
    for ns_part in nsl:
        if not NS_PART_REGEX.match(ns_part):
            raise BadNamespaceException
    return '.'.join(nsl)


class Thing(object):

    """
    HAS_EVENT_HOOK on a Thing class or a descendant of Thing indicates that _event_hook is defined and should be called
                   when a data update arrives for the Thing with these params:
                   @param self - obvious
                   @param is_new - boolean, whether the data is new, - false for snapshots 
                   @param ts - float, unix epoch of the data's timestamp
                   @param data - the data
    """
    HAS_EVENT_HOOK = False

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

    def handle_data_set(self, msg, from_snapshot=False):
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
        if self.thing_class.HAS_EVENT_HOOK:
            _thing._event_hook(not from_snapshot, ts, data)

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
                    self.handle_data_set(data_value, from_snapshot=True)
        else:
            raise BadmessageException("Don't know how to handle message of type %s" % msg['type'])
