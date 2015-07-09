# Things Bus Goals

* Offer a simple entry point to receive data from a variety of sources with minimal configuration (if any)
* Automatically create a directory of information based on the simplest of discovery configuration or inbound push-messages
* Enable distributed local-data-aware programming with a very shallow learning curve

# Things Bus Architecture

Based on ZeroMQ, Things Bus has 3 main parts:

* Pollers/Adaptors - bridges that are aware of both the thing they are tapping into, and Things Bus, so that the individual data sources don't have to be Things Bus aware. The `thingsbus.adaptor` python module supplies an `Adaptor` class which can be used to implement an Adaptor, either as a separate system from the data source, or within it. The Adaptor class uses ZeroMQ to send the updates. Additionally, there is a mode available that sends Messagepack formatted updates via UDP.
* Broker/Directory - Server system that looks at the information gathered by the pollers/adaptos, provides a point from which to receive it using one protocol, and generates a directory of the Things
* Consumers/Clients - Programs that are aware of Things Bus and may use helper libraries supplied by Things Bus, but aren't aware of the implementation details of the Things.  The Clients receive the updates and snapshots over ZeroMQ.
## Implementation Details

### Service Discovery

Both `thingsbus.adaptor.Adaptor` and `thingsbus.client.Client` take the keyword argument `zone`; the SRV record `_thingsbus._tcp` is used to determine the main port & the hostname of the broker server. If this is not desired, then the `broker_url` parameter of `Client` and the `broker_input_url` of `Adaptor` are available for use. It's advised to use an SRV record, for simplicity and for reducing the need for passing additional configuration around to all of the things at the edges of your Thingsbus setup.

### Network Protocol: ZeroMQ TCP + JSON

The Client side protocol and the Adaptor side protocol both support this protocol. The messages are always dictionaries at the top level. The body of the messages contains nothing except uncompressed JSON.

There are 2 message types as of this writing.

#### Thing Update Message

	{
		'type': 'thing_update',
		'ns': dot separated namespace, string,
		'data': [arbitrary data here],
		'ts': float seconds since 1970 UTC,
		'documentation_url': arbitrary string, but should be a URL documenting the format and semantics of 'data'
	}


### Network Protocol: UDP + Messagepack

The Adaptor side protocol supports an additional network protocol, encoding the messages in messagepack instead of JSON and sending them as the entire body of UDP datagrams.

# Examples

## Connect to the broker and get data for a Thing

    >>> import thingsbus.client
    >>> cl = thingsbus.client.Client(zone='pumpingstationone.org')
    >>> elec = cl.directory.get_thing('spacemon.electronics')
    >>> elec.get_data()
    (1.2385549545288086, {u'ratio_busy': 0.03262867647058824, u'luminance': 106.09383138020833})

The last call to `get_data` will return a tuple of float seconds (age of the data) and the data for the directory - if there is any data. If not, None will be returned.

## Print all events from the broker

    (thingsbus)eastein@talisker ~/git/thingsbus :) $ PYTHONPATH=`pwd` python examples/print_all_events.py -u tcp://127.0.0.1:7954
    ThingEvent<Thing<spacemon.electronics>, ts=1423351104.945, flags=, data={u'ratio_busy': 0.14315257352941177, u'luminance': 114.361669921875}>
    ThingEvent<Thing<bottlekid.prototype>, ts=1423351105.715, flags=, data={u'dht22_error': u'bad checksum, DHT22 data: 75:55:77:d5:d5\r\n', u'metadata': {u'loops': 454, u'msgs_dropped': 0, u'msgs_sent': 454}}>



## Run the broker

    python -m thingsbus.broker


## Use the adaptor module

    import thingsbus.adaptor
    adapt = thingsbus.adaptor.Adaptor('shop.shopbot', 'http://example.com/Shopbot_Thingsbus_Documentation', broker_input_url='tcp://*:7955')
    adapt.send({'busy': 12.0, 'light': 31.8}, ns='spacemon')

This sets up an adaptor that lets you send data under the `shop.shopbot` namespace, and then demonstrates sending data for the Thing `shop.shopbot.spacemon` that includes a busy percentage and a light percentage. If ts was supplied (float epoch) to the call to `send`, it would be passed through.

## Adapt lidless at PS1 to a broker

    python -m thingsbus.generic_zmq_adaptor --ns spacemon --nskey camname --tskey frame_time --filter mtype:percept_update --projections luminance,ratio_busy --url 'tcp://*:7955' -s tcp://bellamy.ps1:7202,tcp://bellamy.ps1:7200,tcp://bellamy.ps1:7201,tcp://bellamy.ps1:7206 --documentation "https://wiki.pumpingstationone.org/Spacemon#Things_Bus"


Easy, right?
