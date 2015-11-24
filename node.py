#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import simpy


class Node(object):
    def __init__(self, env, cpu_cores=4, memory=8*1024*1024*1024, disk=320*1024*1024*1024, disk_speed=100*1024*1024):
        "One node is a resouce entity"
        #: need to contend for the CPU resource
        self.cpu_cores = cpu_cores
        self.memory = memory
        self.disk = disk
        self.disk_speed = disk_speed

        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            yield self.env.process(self.map())
            yield self.env.process(self.reduce())

    def map(self):
        print("I am doing map")
        yield self.env.timeout(5)

    def reduce(self):
        print("I am doing reduce")
        yield self.env.timeout(2)


class Mapper(object):
    def __init__(self, env, finish_flag):
        "docstring"
        self.env = env
        self.action = env.process(self.run())
        self.finish_flag = finish_flag

    def run(self):
        while True:
            yield self.env.process(self.map())
            

    def map(self):
        with self.finish_flag.request() as req:
            yield req
            print("start map task")
            yield self.env.timeout(5)
            print("end map task: 5s")

            
class Reducer(object):
    def __init__(self, env, finish_flag):
        "docstring"
        self.env = env
        self.action = env.process(self.run())
        self.finish_flag = finish_flag

    def run(self):
        while True:
            yield self.env.process(self.reduce())

    def reduce(self):
        with self.finish_flag.request() as req:
            yield req
            print("start reduce task")
            yield self.env.timeout(2)
            print("end reduce task: 2s")


def main():
    """Main function only in command line"""
    from sys import argv
    env = simpy.Environment()
    #node = Node(env)

    finish_flag = simpy.Resource(env, capacity=1)
    mapper = Mapper(env, finish_flag)
    env.process(mapper.run())
    reducer = Reducer(env, finish_flag)
    env.process(reducer.run())

    env.run(until=15)
    
    
if __name__ == '__main__':
    main()
