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
        the_hdfs.limplock_create_30_files()

    def test_write_30_64MB_slowdisk(self):
        the_hdfs = hdfs.create_hdfs(number_of_datanodes=11, default_disk_speed=2*1024*1024)
        the_hdfs.limplock_create_30_files()


if __name__ == '__main__':
    unittest.main()
