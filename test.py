#!/usr/bin/python

import logging

import mysqlp


if '__main__' == __name__:
    logging.basicConfig(level=logging.DEBUG)
    conn = mysqlp.connect()
    #c = conn.cursor()
    #c.close()
    conn.close()
