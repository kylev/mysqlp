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


class Logins(unittest.TestCase):
    def test_scramble(self):
        """New style password scramble."""
        password = 'spam'
        seed =  '\x3b\x55\x78\x7d\x2c\x5f\x7c\x72\x49\x52' \
                '\x3f\x28\x47\x6f\x77\x28\x5f\x28\x46\x69'
        hashed = '\x3a\x07\x66\xba\xba\x01\xce\xbe\x55\xe6' \
                 '\x29\x88\xaa\xae\xdb\x00\xb3\x4d\x91\x5b'

        self.assertEqual(hashed, mysqlp._scramble(seed, password))

    def test_scramble_323(self):
        """Old style password scramble."""
        password = 'pass2'
        seed = '\x5a\x3f\x6a\x78\x3c\x62\x5b\x7e'
        hashed = '\x5a\x5d\x4c\x5d\x4e\x43\x42\x4f'

        self.assertEqual(hashed, mysqlp._scramble_323(seed, password))

    def test_password(self):
        """Basic login."""
        c = mysqlp.connect('testuser1', 'pass1', database='mysqlp_test');
        c.close()

    def test_password323(self):
        """Old style password login."""
        c = mysqlp.connect('testuser2', 'pass2', database='mysqlp_test');
        c.close()

    def test_simple_query(self):
        c = mysqlp.connect('testuser1', 'pass1', database='mysqlp_test');
        cur = c.cursor()
        cur.execute('BEGIN')
        cur.execute('SELECT 1')
        cur.fetchall()
        c.commit()
        cur.close()
        c.close()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
