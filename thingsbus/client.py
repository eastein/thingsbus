from zmqfan import zmqsub
import threading
import pprint
from thingsbus import service_discovery, thing


class Thing(thing.Thing):
    pass


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
