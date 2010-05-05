#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="mysqlp",
    version="0.2",
    packages=find_packages(),
    author = "Kyle VanderBeek",
    author_email = "kylev@kylev.com",
    description = "A pure-python MySQL client library.",
    test_suite = 'nose.collector',
)
