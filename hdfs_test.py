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


class TestHDFS(unittest.TestCase):
    def setUp(self):
        env = simpy.Environment()
        self.env = env
        self.switch = node.Switch(self.env)
        self.namenode = node.NameNode(self.env, "namenode")
        self.hdfs = node.HDFS(self.env, namenode=self.namenode, replica_number=3)

        self.switch.add_node(self.namenode)
        self.hdfs.set_namenode(self.namenode)

    def add_datanodes(self, num, disk_speed=80*1024*1024):
        for i in range(11, 11+num):
            datanode = node.Node(self.env, i, disk_speed=disk_speed)
            self.switch.add_node(datanode)
            self.hdfs.add_datanode(datanode)

    def test_basic(self):
        self.add_datanodes(3)
        self.hdfs.start_hdfs_heartbeat()
        self.env.run(300)

    def test_write(self):
        self.add_datanodes(11)

        p = self.hdfs.process_put_file("hello.txt", 100*1024*1024)
        self.env.run(p)

    def test_write_30_64MB(self):
        self.add_datanodes(11)

        self.hdfs.enable_datanode_cache = True
        events = []
        for i in range(30):
            e = self.hdfs.process_put_file("hello.txt", 64*1024*1024)
            events.append(e)
        run_all = AllOf(self.env, events)
        self.env.run(run_all)

    def test_write_30_64MB_slowdisk(self):
        self.add_datanodes(11, disk_speed=2*1024*1024)

        self.hdfs.enable_datanode_cache = True
        events = []
        for i in range(30):
            e = self.hdfs.process_put_file("hello.txt", 64*1024*1024)
            events.append(e)
        run_all = AllOf(self.env, events)
        self.env.run(run_all)


if __name__ == '__main__':
    unittest.main()
