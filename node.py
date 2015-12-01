#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import simpy
from simpy.events import AnyOf


class SimulatorException(Exception):
    pass


env = simpy.Environment()


def debugprint(msg):
    print("[%6.2f] %s" % (env.now, msg))


class Node(object):
    def __init__(self, env, cpu_cores=4, memory=8*1024*1024*1024, disk=320*1024*1024*1024, disk_speed=100*1024*1024):
        "One node is a resouce entity"
        self.env = env

        #: need to contend for the CPU resource
        self.cpu_cores = cpu_cores
        self.memory = memory
        self.disk = disk
        self.disk_speed = simpy.Container(self.env, init=disk_speed, capacity=disk_speed)
        self.disk_events = {}
        self.event_id = 0

    def random_write_tasks(self):
        current_time = 0
        task_time = [1, 1, 2, 3, 3, 3, 4, 9, 9, 9, 30]
        for i in task_time:
            if i == current_time:
                self.new_disk_write_request(1001*1024*1024)
            elif i > current_time:
                try:
                    self.env.run(until=i)
                except SimulatorException as e:
                    print(e)
                self.new_disk_write_request(1001*1024*1024)
            current_time = i

    def new_disk_write_request(self, total_bytes, start_time=0):
        """This is called by client"""
        self.event_id += 1
        event_id = self.event_id
        new_event = self.env.process(self.create_disk_write_event(total_bytes, event_id, start_time))
        self.disk_events[event_id] = new_event

    def create_disk_write_event(self, total_bytes, event_id, start_time=0):
        if start_time > 0:
            yield self.env.timeout(start_time)
        written_bytes = 0
        current_speed = 0

        while written_bytes < total_bytes:
            try:
                if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
                    yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
                #: it should slow down, since we are adding new disk write event
                ideal_speed = int(float(self.disk_speed.capacity) / len(self.disk_events))
                debugprint("%s\tbytes_written: %s/%s\tspeed_request: %s/%s" % (event_id, written_bytes, total_bytes, ideal_speed, self.disk_speed.level))
                # Here is a compromise: if I could not obtain disk right now, I will wait for 1 second
                # then I interrupt others to gain disk
                request_ideal_disk = self.disk_speed.get(ideal_speed)
                timeout = 0.1
                request_timeout = self.env.timeout(timeout)

                try:
                    yield request_ideal_disk | request_timeout
                except simpy.Interrupt as e:
                    debugprint(e)
                    continue
                except SimulatorException as e:
                    print(e)
                    continue

                debugprint("%s\tDiskRequestSuccess?: %s\t%s" % (event_id, request_ideal_disk.processed, self.disk_speed.level))
                if request_ideal_disk.processed: # I have got disk!
                    current_speed = ideal_speed
                    estimated_finish_time = (total_bytes - written_bytes) / current_speed
                    debugprint("speed/estimated_finish_time: %6.2f/%6.2f" % (current_speed, estimated_finish_time + self.env.now))
                    try:
                        yield self.env.timeout(estimated_finish_time)
                    except simpy.Interrupt as e:
                        written_bytes += current_speed * (self.env.now - e.cause['time'])
                        debugprint("%s is interrupted %s: %s/%s" % (event_id, e, written_bytes, total_bytes))
                        continue
                    break
                else: # interrupt others!
                   # try:
                   #     request_ideal_disk.fail(SimulatorException("fail myself"))
                   # except SimulatorException as e:
                   #     print(e)
                    if self.disk_speed.capacity > self.disk_speed.level:
                        yield self.disk_speed.put(self.disk_speed.capacity - self.disk_speed.level)
                    debugprint("%s\tis interrupting others!" % event_id)
                    for k, e in self.disk_events.items():
                        if k != event_id:
                            e.interrupt({"info": "make a space for new task", "time": self.env.now - timeout})
            except simpy.Interrupt as e:
                raise e("it should not happen")

        # event finished
        self.disk_events.pop(event_id)
        debugprint("%s\tFINISHED\t%s B written" % (event_id, total_bytes))


def main():
    """Main function only in command line"""
    from sys import argv
    node = Node(env)

    node.random_write_tasks()

    env.run()
    
    
if __name__ == '__main__':
    main()
