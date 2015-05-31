from __future__ import absolute_import
import time
import threading
import copy
import pprint
import re
import six


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

    def __init__(self, directory, ns):
        self.directory = directory
        self.ns = ns
        self.ns_parts = parse_ns(self.ns)
        self.children = list()

        if self.ns:
            # all but the root have a parent, and should register themselves with their parent
            self.parent = self.directory.get_thing(stringify_ns(self.ns_parts[0:-1]))
            self.parent._register_child(self)
        else:
            # the root has no parent.
            self.parent = None

        self.data_lock = threading.Lock()
        self.last_data = None
        self.last_data_ts = None
        self.documentation_url = None

    def _register_child(self, child):
        """
        This call is for internal use; it tells a Thing that it has a child Thing.
        """
        self.children.append(child)

    def set_data(self, data, ts, documentation_url=None):
        with self.data_lock:
            self.last_data = data
            if documentation_url:
                self.documentation_url = documentation_url
            self.last_data_ts = ts

    def get_data(self):
        now = time.time()
        with self.data_lock:
            if self.last_data_ts is None:
                return None

            return (now - self.last_data_ts), copy.deepcopy(self.last_data)

    def __repr__(self):
        if self.ns:
            return 'Thing<%s>' % self.ns
        else:
            return 'RootThing'

    def __str__(self):
        return repr(self)


class Directory(object):

    def __init__(self, thing_class=None):
        self._name_to_thing = dict()
        if thing_class is None:
            thing_class = thing.Thing
        self.thing_class = thing_class
        self.root = self.get_thing('')

    def get_thing(self, ns, create_on_missing=True):
        if ns not in self._name_to_thing:
            if create_on_missing:
                self._name_to_thing[ns] = self.thing_class(self, ns)
            else:
                return

        return self._name_to_thing[ns]

    @property
    def all_things(self):
        return list(self._name_to_thing.values())

    def handle_data_set(self, msg, from_snapshot=False):
        for k in ['ns', 'data']:
            if k not in msg:
                raise BadMessageException("%s not in message." % k)

        # TODO validate ns, ts - data really can be anything.
        ns = msg['ns']
        if not isinstance(ns, six.string_types) :
            raise BadMessageException("ns was %s." % type(ns))
        ts = msg.get('ts', None)
        if ts is None:
            ts = time.time()
        elif not isinstance(ts, float):
            raise BadMessageException("ts was not None or float in input.")
        documentation_url = msg.get('documentation_url')
        data = msg['data']

        # evidently we'll overwrite our own module if we use the same identifier. Fun! Do not do that.
        _thing = self.get_thing(ns)
        _thing.set_data(data, ts, documentation_url=documentation_url)
        if self.thing_class.HAS_EVENT_HOOK:
            _thing._event_hook(not from_snapshot, ts, data)

        return {
            'type': 'thing_update',
            'ns': ns,
            'data': data,
            'ts': ts,
            'documentation_url': documentation_url
        }

    def handle_message(self, msg, accept_snapshots=False, accept_listmsg=False):
        if not accept_snapshots:
            if accept_listmsg:
                if type(msg) == list:
                    if len(msg) != 4:
                        raise BadMessageException("List typed messages must be of length 4.")

                    # accepting no snapshots + accepting list messages means we are accepting smaller,
                    # simpler-encoding lists [ns, ts, data, documentation_url] where ts MAY be
                    # null and documentation_url SHOULD not be null
                    msg = {
                        'ns': msg[0],
                        'ts': msg[1],
                        'data': msg[2],
                        'documentation_url': msg[3],
                        'type': 'thing_update'
                    }

        if type(msg) != dict:
            raise BadMessageException("Dictionaries are the only valid top level message.")

        if 'type' not in msg:
            raise BadMessageException("Messages must be typed.")

        if msg['type'] == 'thing_update':
            try:
                return self.handle_data_set(msg)
            except BadNamespaceException:
                pass  # TODO handle debug printing this
        elif msg['type'] == 'thing_snapshot':
            if accept_snapshots:
                if 'data' not in msg:
                    raise BadMessageException("Snapshot must have a data key.")
                if type(msg['data']) != dict:
                    raise BadMessageException("Snapshot data must be a dictionary.")

                for data_value in list(msg['data'].values()):
                    try:
                        self.handle_data_set(data_value, from_snapshot=True)
                    except BadNamespaceException:
                        pass  # TODO handle debug printing this
        else:
            raise BadMessageException("Don't know how to handle message of type %s" % msg['type'])
