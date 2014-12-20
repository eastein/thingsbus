import unittest

from thingsbus import thing


class NSTests(unittest.TestCase):

    def test_roots(self):
        self.assertEquals(thing.stringify_ns([]), '')
        self.assertEquals(thing.parse_ns(''), [])

    def test_simple(self):
        self.assertEquals(thing.parse_ns('a'), ['a'])
        self.assertEquals(thing.parse_ns('a.b'), ['a', 'b'])

    def test_bad_namespaces(self):
        self.assertRaises(thing.BadNamespaceException, thing.parse_ns, 'A')
        self.assertRaises(thing.BadNamespaceException, thing.parse_ns, 'a.B')
        self.assertRaises(thing.BadNamespaceException, thing.parse_ns, 'a.')
        self.assertRaises(thing.BadNamespaceException, thing.parse_ns, '.a')

    def test_bad_namespaces_stringify(self):
        self.assertRaises(thing.BadNamespaceException, thing.stringify_ns, [''])
        self.assertRaises(thing.BadNamespaceException, thing.stringify_ns, ['a', ' b'])
