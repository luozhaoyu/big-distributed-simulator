#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"


import random
import time
import unittest

import hdfs


class TestReport(unittest.TestCase):

    @unittest.skip("too slow")
    def test_regenerate_block(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20)
        print("RegerateBlocks20Datanodes\tExpectationTime\tExecutionTime")
        prev = the_hdfs.env.now
        l = list(range(30))
        l.extend([50, 100, 200])
        for i in l:
            start = time.time()
            t = the_hdfs.regenerate_blocks(i)
            end = time.time()
            print("%i\t%.1f\t%.3f" % (i, t-prev, end - start))
            prev = t

    @unittest.skip("too slow")
    def test_create_files(self):
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20)
        print("CreateFiles20Datanodes\tExpectationTime\tExecutionTime")
        prev = the_hdfs.env.now
        l = list(range(30))
        l.extend([50, 100, 200])
        for i in l:
            start = time.time()
            t = the_hdfs.put_files(i, 64*1024*1024)
            end = time.time()
            print("%i\t%.1f\t%.3f" % (i, t-prev, end - start))
            prev = t

    @unittest.skip("too slow")
    def test_number_of_datanodes(self):
        print("NumberOfDatanodes\tExpectationTimeRegerate30\tExecutionTime")
        l = [5, 10, 20, 40, 80, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        for i in l:
            the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=i)
            start = time.time()
            t = the_hdfs.regenerate_blocks(30)
            end = time.time()
            print("%i\t%.1f\t%.3f" % (i, t, end - start))

    def test_replica_number(self):
        print("ReplicaNumber\tExpectationTimeRegerate30\tExecutionTime")
        l = range(10)
        for i in l:
            the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20, replica_number=i)
            start = time.time()
            t = the_hdfs.put_files(30, 64*1024*1024)
            end = time.time()
            print("%i\t%.1f\t%.3f" % (i, t, end - start))

    def test_heartbeats(self):
        print("HeartbeatInteval\tHeartbeatSize\tExpectationTimeRegerate30\tExecutionTime")
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%i\t%i\t%.1f\t%.3f" % (3, 1024, t, end - start))
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20, heartbeat_interval=1, heartbeat_size=1024*1024)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%i\t%i\t%.1f\t%.3f" % (30, 32*1024*1024, t, end - start))
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20, enable_heartbeats=False, enable_block_report=False)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%i\t%i\t%.1f\t%.3f" % (0, 0, t, end - start))

    def test_client_write_packet_size(self):
        print("ClientWritePacket\tExpectationTimeRegerate30\tExecutionTime")
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%i\t%.1f\t%.3f" % (1024*1024, t, end - start))
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20, client_write_packet_size=256*1024)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%i\t%.1f\t%.3f" % (256*1024, t, end - start))

    def test_enable_datanode_cache(self):
        print("EnableDatanodeCache\tExpectationTimeRegerate30\tExecutionTime")
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%s\t%.1f\t%.3f" % (True, t, end - start))
        the_hdfs = hdfs.create_silent_hdfs(number_of_datanodes=20, enable_datanode_cache=False)
        start = time.time()
        t = the_hdfs.regenerate_blocks(30)
        end = time.time()
        print("%s\t%.1f\t%.3f" % (False, t, end - start))


if __name__ == '__main__':
    unittest.main()
