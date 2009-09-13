#!/usr/bin/python

import unittest

import mysqlp


class UtilTest(unittest.TestCase):
    def test_lencode_1(self):
        self.assertEqual(mysqlp._encode_len(1), '\x01')

    def test_lencode_2(self):
        self.assertEqual(mysqlp._encode_len(250), '\xfa')


class BinEncoding(unittest.TestCase):
    def test_NULL(self):
        self.assertEqual(mysqlp._len_bin(None), '\xfb')


if __name__ == '__main__':
    unittest.main()
