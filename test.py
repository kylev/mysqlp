#!/usr/bin/python

import logging

import mysqlp


if '__main__' == __name__:
    logging.basicConfig(level=logging.DEBUG)
    conn = mysqlp.connect(user='kylev', db='test')
    c = conn.cursor()
    c.execute('SELECT 1, 2, 4')
    print c.fetchall()
    c.close()
    conn.close()
