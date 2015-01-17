#!usr/bin/env python

import optparse
from zmqfan import zmqsub
import thingsbus.client
import socket
import time

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='thingsbus_broker_url', default=None,
                      help="ZMQ url to connect to - things bus broker.")
    parser.add_option('-z', '--zone', dest='thingsbus_zone', default=None, help='Thingsbus zone.')
    parser.add_option('--namespace', '-n', dest='ns', default=None,
                      help="Namespace to subscribe to (TREE mode - everything under that as well) - if not given, will use root namespcae.")

    (opts, args) = parser.parse_args()

    client = thingsbus.client.Client(broker_url=opts.thingsbus_broker_url, zone=opts.thingsbus_zone)

    def print_e(e):
        print e

    if opts.ns:
        thing = client.directory.get_thing(opts.ns)
    else:
        thing = client.directory.root

    thing.subscribe(print_e, thingsbus.client.F_TREE)

    while True:
        time.sleep(1)
