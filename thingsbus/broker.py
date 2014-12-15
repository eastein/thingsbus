from zmqfan import zmqsub
import time

"""
General process for the broker: given the set of things you have, it will keep the latest data for each item, send snapshots on a regular pattern, etc - it also does fan-out, passing information to however many subscribers there are.
"""

INPUT_PORT = 7955
DIRECTORY_PORT = 7954
# tune these TODO
DIRECTORY_INTERVAL = 15
DIRECTORY_EXPIRE = 60
BLOCK_TIME = 0.05


class Thing(object):

    """
    namespace is a dot delimited java-package type of thing. The name of the Thing itself should appear last.
    """

    def __init__(self, namespace):
        self.namespace = namespace
        self.last_data = None
        self.last_data_ts = None

    def set_data(self, data, ts):
        self.last_data = data
        self.last_data_ts = ts

    def emit_snapshot(self):
        return {
            'data': self.last_data,
            'ts': self.last_data_ts,
            'ns': self.namespace,
        }

    @property
    def expired(self):
        if self.last_data_ts is None:
            return True
        elif self.last_data_ts < (time.time() - DIRECTORY_EXPIRE):
            return True
        else:
            return False


class Directory(object):

    def __init__(self):
        self.name_to_thing = dict()

    def get_thing(self, ns):
        if ns not in self.name_to_thing:
            self.name_to_thing[ns] = Thing(ns)

        return self.name_to_thing[ns]


class Broker(object):

    def __init__(self):
        self.ok = True

    def stop(self):
        self.ok = False

    def run(self):
        self.directory = Directory()
        self.directory_out = zmqsub.BindPub('tcp://*:%d' % DIRECTORY_PORT)
        self.adaptors_in = zmqsub.BindSub('tcp://*:%d' % INPUT_PORT)
        self.sent_directory = time.time() - DIRECTORY_INTERVAL
        while self.ok:
            try:
                msg = self.adaptors_in.recv(timeout=BLOCK_TIME)
                # TODO checking keys, there are likely some remotely exploitable easy crashes right here

                if msg['type'] == 'thing_update':
                    try:
                        ns = msg['ns']
                        data = msg['data']
                        ts = msg.get('ts', time.time())
                    except KeyError:
                        # well this is screwed! Let's give up.
                        continue

                    thing = self.directory.get_thing(ns)
                    thing.set_data(data, ts)

                    self.directory_out.send({
                        'type': 'thing_update',
                        'ns': ns,
                        'data': data,
                        'ts': ts
                    })

            except zmqsub.NoMessagesException:
                pass  # it's ok to not receive anything (for christmas, or on sockets)

            now = time.time()
            if now > self.sent_directory + DIRECTORY_INTERVAL:
                # time to send out a snapshot, for fun!
                # maybe we should send snapshots on another socket, later.
                self.directory_out.send({
                    'type': 'thing_snapshot',
                    'ts': now,
                    'data': dict([
                        (ns, thing.emit_snapshot())
                        for
                        (ns, thing)
                        in
                        self.directory.name_to_thing.items()
                        if not thing.expired
                    ])
                })
                self.sent_directory = now

if __name__ == '__main__':
    broker = Broker()
    try:
        broker.run()
    except KeyboardInterrupt:
        broker.stop()
