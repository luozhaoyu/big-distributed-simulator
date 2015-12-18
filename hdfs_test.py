#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import unittest

import simpy
from simpy.events import AllOf

import hdfs


class TestHDFS(unittest.TestCase):


    def test_basic(self):
        the_hdfs = hdfs.create_silent_hdfs()
        the_hdfs.run_until(300)

    def test_lots_datanodes(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=100)
        the_hdfs.run_until(1000)

    def test_write(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=10)
        the_hdfs.put_files(1, 100*1024*1024)

    def test_create_files(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=10)
        the_hdfs.put_files(2, 64*1024*1024)

    def test_create_lots_files(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=10)
        the_hdfs.put_files(2, 64*1024*1024)


if __name__ == '__main__':
    unittest.main()
