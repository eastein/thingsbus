# Things Bus Goals

* Offer a simple entry point to receive data from a variety of sources with minimal configuration (if any)
* Automatically create a directory of information based on the simplest of discovery configuration or inbound push-messages
* Enable distributed local-data-aware programming with a very shallow learning curve

# Things Bus Architecture

Based on ZeroMQ, Things Bus has 3 main parts:

* Pollers/Adaptors - bridges that are aware of both the thing they are tapping into, and Things Bus, so that the individual data sources don't have to be Things Bus aware. The `thingsbus.adaptor` python module supplies an `Adaptor` class which can be used to implement an Adaptor, either as a separate system from the data source, or within it.
* Broker/Directory - Server system that looks at the information gathered by the pollers/adaptos, provides a point from which to receive it using one protocol, and generates a directory of the Things
* Consumers/Clients - Programs that are aware of Things Bus and may use helper libraries supplied by Things Bus, but aren't aware of the implementation details of the Things.

## Implemtation Details

### Service Discovery

Both `thingsbus.adaptor.Adaptor` and `thingsbus.client.Client` take the keyword argument `zone`; the SRV record `_thingsbus._tcp` is used to determine the main port & the hostname of the broker server. If this is not desired, then the `broker_url` parameter of `Client` and the `broker_input_url` of `Adaptor` are available for use. It's advised to use an SRV record, for simplicity and for reducing the need for passing additional configuration around to all of the things at the edges of your Thingsbus setup.

# Examples

## Adapt lidless at PS1 to a broker

    python -m thingsbus.generic_zmq_adaptor --ns spacemon --nskey camname --tskey frame_time --filter mtype:percept_update --projections luminance,ratio_busy --url 'tcp://*:7955' -s tcp://bellamy.ps1:7202,tcp://bellamy.ps1:7200,tcp://bellamy.ps1:7201,tcp://bellamy.ps1:7206


Easy, right?

## Run the broker

    python -m thingsbus.broker


## Use the adaptor module

    import thingsbus.adaptor
    adapt = thingsbus.adaptor.Adaptor('shop.shopbot', broker_input_url='tcp://*:7955')
    adapt.start()
    adapt.send({'busy': 12.0, 'light': 31.8}, ns='spacemon')

This sets up an adaptor that lets you send data under the `shop.shopbot` namespace, and then demonstrates sending data for the Thing `shop.shopbot.spacemon` that includes a busy percentage and a light percentage. If ts was supplied (float epoch) to the call to `send`, it would be passed through.



## Connect to the broker and get data for a Thing

    >>> import thingsbus.client
    >>> cl = thingsbus.client.Client(zone='pumpingstationone.org')
    >>> cl.start()
    >>> elec = cl.directory.get_thing('spacemon.electronics')
    >>> elec.get_data()
    (1.2385549545288086, {u'ratio_busy': 0.03262867647058824, u'luminance': 106.09383138020833})

The lats call to `get_data` will return a tuple of float seconds (age of the data) and the data for the directory - if there is any data. If not, None will be returned.
