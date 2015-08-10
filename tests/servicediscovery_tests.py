import unittest
import thingsbus.service_discovery
import mock


class SDTests(unittest.TestCase):

    @mock.patch('thingsbus.service_discovery.ServiceFinder._srv_query')
    def test_broker_output_url(self, srv_query):
        srv_query.return_value = ('broker.example.com', 7954)
        self.assertEquals(
            'tcp://broker.example.com:7954', thingsbus.service_discovery.ServiceFinder.broker_url('example.com'))
        srv_query.assert_called_with('_thingsbus._tcp.example.com')

    @mock.patch('thingsbus.service_discovery.ServiceFinder._srv_query')
    def test_broker_input_url(self, srv_query):
        srv_query.return_value = ('broker.example.com', 7955)
        self.assertEquals('tcp://broker.example.com:7955',
                          thingsbus.service_discovery.ServiceFinder.broker_input_url('example.com'))
        srv_query.assert_called_with('_thingsbus_input._tcp.example.com')

    @mock.patch('thingsbus.service_discovery.ServiceFinder._srv_query')
    def test_broker_input_udp_url(self, srv_query):
        srv_query.return_value = ('broker.example.com', 7955)
        self.assertEquals('tcp://broker.example.com:7955',
                          thingsbus.service_discovery.ServiceFinder.broker_input_url('example.com', protocol='udp'))
        srv_query.assert_called_with('_thingsbus_input._udp.example.com')
