import time


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


class Directory(object):

    def __init__(self, thing_class=None):
        self.name_to_thing = dict()
        if thing_class is None:
            thing_class = Thing
        self.thing_class = thing_class

    def get_thing(self, ns):
        if ns not in self.name_to_thing:
            self.name_to_thing[ns] = self.thing_class(ns)

        return self.name_to_thing[ns]
