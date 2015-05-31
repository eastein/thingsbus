from __future__ import absolute_import
from zmqfan import zmqsub
import threading
import pprint
from thingsbus import service_discovery, thing

F_NONE = 0
F_SNAPSHOT = 1
F_TREE = 2
FLAG_REPR = [
    (F_SNAPSHOT, 'F_SNAPSHOT'),
    (F_TREE, 'F_TREE'),
]


def repr_flag(flags):
    return '|'.join([fr[1] for fr in FLAG_REPR if (fr[0] & flags) != 0])


def chk_flag(flags, flag):
    return (flags & flag) != 0


class ThingEvent(object):

    def __init__(self, thing_obj, ts, data, flags):
        self.thing = thing_obj
        self.ts = ts
        self.data = data
        self.flags = flags

    def __repr__(self):
        return 'ThingEvent<%s, ts=%0.3f, flags=%s, data=%s>' % (repr(self.thing), self.ts, repr_flag(self.flags), repr(self.data))

    @property
    def is_snapshot(self):
        return chk_flag(self.flags, F_SNAPSHOT)


class Thing(thing.Thing):
    HAS_EVENT_HOOK = True

    def __init__(self, *a, **kw):
        thing.Thing.__init__(self, *a, **kw)
        self.event_listeners = list()

    def subscribe(self, callable_function, flags=F_NONE):
        self.event_listeners.append((callable_function, flags))

    def unsubscribe(self, callable_function):
        self.event_listeners = [elt for elt in self.event_listeners if elt[0] != callable_function]

    def _event_handle(self, event, tree_mode):
        flags_check = []
        if event.is_snapshot:
            flags_check.append(F_SNAPSHOT)
        if tree_mode:
            flags_check.append(F_TREE)

        for el, flags in self.event_listeners:
            process = True
            for flag_check in flags_check:
                if not chk_flag(flags, flag_check):
                    process = False
                    break
            if process:
                el(event)

        if self.parent is not None:
            self.parent._event_handle(event, True)

    def _event_hook(self, is_new, ts, data):
        flags = F_NONE
        if not is_new:
            flags |= F_SNAPSHOT
        event = ThingEvent(self, ts, data, flags)
        self._event_handle(event, False)


class Directory(thing.Directory):

    def __init__(self):
        thing.Directory.__init__(self, thing_class=Thing)


class Client(threading.Thread):

    def __init__(self, zone=None, broker_url=None, context=None):
        """
        @param context a zeromq context or zmqsub.JSONZMQ object to get context from; otherwise create a new one.
        @param zone the zone to use SRV records to locate the broker for
        @param broker_url the broker url to use, if you'd prefer not to use service discovery
        """
        self.ok = True
        self.zone = zone
        self._broker_url = broker_url
        self.directory = Directory()
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    @property
    def broker_url(self):
        if self._broker_url is not None:
            return self._broker_url
        elif self.zone is not None:
            return service_discovery.ServiceFinder.broker_url(self.zone)

    def stop(self):
        self.ok = False

    def run(self):
        conn = zmqsub.ConnectSub(self.broker_url)
        while self.ok:
            try:
                msg = conn.recv(timeout=0.05)

                self.directory.handle_message(msg, accept_snapshots=True)
            except thing.BadMessageException:
                pass  # TODO print during debug mode...
            except zmqsub.NoMessagesException:
                pass
