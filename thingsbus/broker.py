import optparse
from thingsbus import thing
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


class BrokerThing(thing.Thing):

    def emit_snapshot(self):
        with self.data_lock:
            return {
                'data': self.last_data,
                'ts': self.last_data_ts,
                'ns': self.ns,
                'documentation_url': self.documentation_url
            }

    @property
    def expired(self):
        if self.last_data_ts is None:
            return True
        elif self.last_data_ts < (time.time() - DIRECTORY_EXPIRE):
            return True
        else:
            return False


class Broker(object):

    def __init__(self, verbose=False):
        self.directory = thing.Directory(thing_class=BrokerThing)
        self.ok = True
        self.verbose = verbose

    def stop(self):
        self.ok = False

    def run(self):
        self.directory_out = zmqsub.BindPub('tcp://*:%d' % DIRECTORY_PORT)
        self.adaptors_in = zmqsub.BindSub('tcp://*:%d' % INPUT_PORT)
        self.sent_directory = time.time() - DIRECTORY_INTERVAL
        while self.ok:
            try:
                msg = self.adaptors_in.recv(timeout=BLOCK_TIME)

                output_event = self.directory.handle_message(msg)
                if output_event:
                    self.directory_out.send(output_event)
            except thing.BadMessageException:
                if self.verbose:
                    print 'recvd bad zeromq message, skipped.'
            except zmqsub.NoMessagesException:
                pass  # it's ok to not receive anything (for christmas, or on sockets)

            now = time.time()
            if now > self.sent_directory + DIRECTORY_INTERVAL:
                # time to send out a snapshot, for fun!
                # maybe we should send snapshots on another socket, later.
                snapshot_msg = {
                    'type': 'thing_snapshot',
                    'ts': now,
                    'data': dict([
                        (thing_obj.ns, thing_obj.emit_snapshot())
                        for
                        thing_obj
                        in
                        self.directory.all_things
                        if not thing_obj.expired
                    ])
                }
                self.directory_out.send(snapshot_msg)
                if self.verbose:
                    print 'sent snapshot of %d things.' % len(snapshot_msg['data'])
                self.sent_directory = now

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true', default=False, help="Verbose mode.")

    (opts, args) = parser.parse_args()

    broker_obj = Broker(verbose=opts.verbose)
    try:
        broker_obj.run()
    except KeyboardInterrupt:
        broker_obj.stop()
