#!usr/bin/env python

import optparse
from zmqfan import zmqsub
import thingsbus.adaptor

"""
ns_key = camname
ts_key = frame_time

filter = mtype:percept_update

projections = luminance,ratio_busy
"""

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='thingsbus_broker_input_url', default=None,
                      help="ZMQ url to connect to - things bus broker input.")
    parser.add_option('-z', '--zone', dest='thingsbus_zone', default=None, help='Thingsbus zone.')
    parser.add_option('-s', '--sources', dest='source_urls',
                      help="ZMQ urls to connect to (they should be the bind end of PUB/SUB) to receive the messages.")
    parser.add_option('--tskey', dest='tskey', default=None,
                      help="The key in the messages to use to get the time. Optional. Should be a float of seconds since 1970.")
    parser.add_option('--nskey', dest='nskey', default=None,
                      help="The key in the messages to use to get the final part of the namespace from. Optional.")
    parser.add_option('--filter', dest='filter', default=None,
                      help=": separated - the key and the value to check-equals to. Use for limiting the flow of events into things bus.")
    parser.add_option('--projections', dest='projections', default=None,
                      help='Comma separated list of fields to include in the data dictionary into things bus.')
    parser.add_option('--ns', dest='ns', help='Namespace to put information into')
    parser.add_option('-d', '--documentation', dest='documentation_url', help="Documentation URL.")

    (opts, args) = parser.parse_args()

    sources = list()
    source = None
    for url in opts.source_urls.split(','):
        source = zmqsub.ConnectSub(url, context=source)
        sources.append(source)

    adaptor = thingsbus.adaptor.Adaptor(
        opts.ns, opts.documentation_url, broker_input_url=opts.thingsbus_broker_input_url, zone=opts.thingsbus_zone)

    try:
        while True:
            for source in sources:
                try:
                    msg = source.recv(timeout=0.01)

                    if opts.filter is not None:
                        k, v = opts.filter.split(':', 1)

                        if k not in msg:
                            continue
                        if msg[k] != v:
                            continue

                    ns = None
                    if opts.nskey is not None:
                        ns = msg[opts.nskey]
                    ts = None
                    if opts.tskey is not None:
                        ts = msg[opts.tskey]

                    data = dict()
                    for p in opts.projections.split(','):
                        data[p] = msg[p]

                    adaptor.send(data, ns=ns, ts=ts)
                except zmqsub.NoMessagesException:
                    pass
    except KeyboardInterrupt:
        adaptor.stop()
