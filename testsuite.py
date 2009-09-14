#!/usr/bin/python

import unittest

import mysqlp
from mysqlp import wire

class UtilTest(unittest.TestCase):
    def test_lencode_1(self):
        self.assertEqual(mysqlp._encode_len(1), '\x01')

    def test_lencode_2(self):
        self.assertEqual(mysqlp._encode_len(250), '\xfa')


class WireFormats(unittest.TestCase):
    def test_encode_int_1(self):
        self.assertEqual(wire.encode_int(1), '\x01')
        self.assertEqual(wire.encode_int(1, 1), '\x01')

    def test_encode_int_padded(self):
        """Padding is in the correct place."""
        self.assertEqual(wire.encode_int(1, 2), '\x01\x00')
        self.assertEqual(wire.encode_int(1, 3), '\x01\x00\x00')

    def test_int_symmetry(self):
        """encode/decode_int should work back and forth."""
        result = wire.encode_int(99)
        self.assertEqual(wire.decode_int(result), (99, ''))


class BinEncoding(unittest.TestCase):
    def test_NULL(self):
        self.assertEqual(mysqlp._len_bin(None), '\xfb')


if __name__ == '__main__':
    unittest.main()
