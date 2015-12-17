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

    def create_silent_hdfs(self, **kwargs):
        return hdfs.create_hdfs(do_debug=False, do_info=False, do_warning=True, do_critical=True, **kwargs)

    def test_basic(self):
        the_hdfs = self.create_silent_hdfs()
        the_hdfs.run_until(300)

    def test_write(self):
        the_hdfs = self.create_silent_hdfs(number_of_datanodes=11)
        p = the_hdfs.process_put_file("hello.txt", 100*1024*1024)
        the_hdfs.run_until(p)

    def test_limplock_create_30_64MB(self):
        the_hdfs = self.create_silent_hdfs(number_of_datanodes=40)
        the_hdfs.limplock_create_30_files()

    def test_limplock_create_30_64MB_clusterlimp(self):
        the_hdfs = self.create_silent_hdfs(number_of_datanodes=40, default_disk_speed=2*1024*1024)
        the_hdfs.limplock_create_30_files()

    def test_limplock_regenerate_90_blocks_limpfree(self):
        the_hdfs = self.create_silent_hdfs(number_of_datanodes=40)
        the_hdfs.limplock_regenerate_90_blocks()


if __name__ == '__main__':
    unittest.main()
