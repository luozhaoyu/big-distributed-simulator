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

import node
import hdfs


class TestHDFS(unittest.TestCase):

    def test_basic(self):
        the_hdfs = hdfs.create_hdfs()
        the_hdfs.run_until(300)

    def test_write(self):
        the_hdfs = hdfs.create_hdfs(number_of_datanodes=11)
        p = the_hdfs.process_put_file("hello.txt", 100*1024*1024)
        the_hdfs.run_until(p)

    def test_write_30_64MB(self):
        the_hdfs = hdfs.create_hdfs(number_of_datanodes=11)
        events = []
        for i in range(30):
            e = the_hdfs.process_put_file("hello.txt", 64*1024*1024)
            events.append(e)
        run_all = AllOf(the_hdfs.env, events)

        the_hdfs.run_until(run_all)

    def test_write_30_64MB_slowdisk(self):
        the_hdfs = hdfs.create_hdfs(number_of_datanodes=11, default_disk_speed=2*1024*1024)
        events = []
        for i in range(30):
            e = the_hdfs.process_put_file("hello.txt", 64*1024*1024)
            events.append(e)
        run_all = AllOf(the_hdfs.env, events)

        the_hdfs.run_until(run_all)


if __name__ == '__main__':
    unittest.main()
