#!/usr/bin/python

import logging

import pMySQL


if '__main__' == __name__:
    logging.basicConfig(level=logging.DEBUG)
    conn = pMySQL.connect('localhost', 'sbrsw_write', 'wonkette', 'sbrsw')
    #c = conn.cursor()
    #c.close()
    conn.close()
