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

import node

class TestDefault(unittest.TestCase):
    def setUp(self):
        env = simpy.Environment()
        self.env = env
        self.node = node.Node(self.env, 1)
        self.node2 = node.Node(self.env, 2)

    def test_disk_write(self):
        def random_write_tasks(node):
            task_time = [1, 1, 2, 3, 3, 3, 4, 9, 9, 9, 30]
            for i in task_time:
                node.new_disk_write_request(1001*1024*1024, i)

        random_write_tasks(self.node)
        self.node.process_break_disk(50)
        self.node.process_repair_disk(80)
        self.env.run(190)

    def test_disk_buffer_write(self):
        def random_write_tasks(node):
            task_time = [1, 1, 2, 3, 3, 3, 4, 9, 9, 9, 30]
            for i in task_time:
                node.new_disk_buffer_write_request(1001*1024*1024, i)

        random_write_tasks(self.node)
        self.env.run(290)


class TestSwitch(unittest.TestCase):
    def setUp(self):
        env = simpy.Environment()
        self.env = env
        self.node = node.Node(self.env, 1)
        self.node2 = node.Node(self.env, 2)
        self.switch = node.Switch(self.env)

    def test_ping(self):
        self.switch.add_node(self.node)
        self.switch.add_node(self.node2)
        self.switch.heartbeat_ping(self.node.id, self.node2.id, 100*1024*1024)

        self.env.run(300)


if __name__ == '__main__':
    unittest.main()
