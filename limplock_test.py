#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"


import unittest

import hdfs


class TestHDFS(unittest.TestCase):

    def test_limplock_create_30_64MB(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=40)
        the_hdfs.limplock_create_30_files()

    def test_limplock_create_30_64MB_clusterlimp(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=40, default_disk_speed=2*1024*1024)
        the_hdfs.limplock_create_30_files()

    def test_limplock_regenerate_90_blocks_limpfree(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=40)
        the_hdfs.limplock_regenerate_90_blocks()


if __name__ == '__main__':
    unittest.main()
