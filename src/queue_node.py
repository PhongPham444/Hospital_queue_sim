# src/queue_node.py
import simpy
import random
from typing import Dict, List

class QueueNode:
    def __init__(self, env: simpy.Environment, name: str, service_rate: float, servers: int):
        self.env = env
        self.name = name
        self.service_rate = float(service_rate)  # mu per server
        self.servers = int(servers)
        self.resource = simpy.Resource(env, capacity=self.servers)
        # area integrals and bookkeeping
        self.last_event_time = 0.0
        self.queue_area = 0.0       # integral of queue_length(t) dt
        self.busy_area = 0.0        # integral of in_service(t) dt
        self.current_in_service = 0 # number of servers busy at current time
        self.system_area= 0.0
        # queue log (time, q_len) for optional post-checking
        self.queue_log: List[tuple] = []  # list of (time, queue_length)
        # count completed jobs
        self.completed_jobs = 0
        # set last_event_time to env.now initially
        self.last_event_time = env.now

    def _update_areas(self):
        now = self.env.now
        delta = now - self.last_event_time
        if delta < 0:
            delta = 0.0
        # queue length is number waiting in resource.queue
        q_len = len(self.resource.queue)
        self.queue_area += q_len * delta
        self.busy_area += self.current_in_service * delta
        self.system_area += (q_len + self.current_in_service) * delta
        self.last_event_time = now
        # optional log
        self.queue_log.append((now, q_len))

    def _sample_service_time(self) -> float:
        # Use Python's random.expovariate(lambda) where lambda = service_rate
        if self.service_rate <= 0:
            return 0.0
        return random.expovariate(self.service_rate)

    def serve(self, patient):
        """
        This is a generator to be used as env.process(node.serve(patient))
        It records patient arrival, service start, service end, and updates areas.
        """
        # patient arrives to this node (enters queue)
        patient.record_arrival(self.name, self.env.now)
        # update areas up to now BEFORE changing counters
        self._update_areas()

        with self.resource.request() as req:
            yield req  # wait for a free server
            # just before starting service
            self._update_areas()
            # service start
            patient.record_service_start(self.name, self.env.now)
            # update counters
            self.current_in_service += 1
            # update areas after change
            self._update_areas()

            # actual service time
            service_time = self._sample_service_time()
            if service_time > 0:
                yield self.env.timeout(service_time)
            else:
                yield self.env.timeout(0)

            # service ends
            patient.record_service_end(self.name, self.env.now)
            # update counters
            self.current_in_service -= 1
            self.completed_jobs += 1
            # update area after finishing
            self._update_areas()

    def finalize(self, sim_end_time: float):
        """
        Ensure we account for tail interval until sim_end_time
        """
        now = self.env.now
        if sim_end_time > self.last_event_time:
            delta = sim_end_time - self.last_event_time
            q_len = len(self.resource.queue)
            self.queue_area += q_len * delta
            self.busy_area += self.current_in_service * delta
            self.last_event_time = sim_end_time

    def avg_queue_length(self, effective_time: float) -> float:
        if effective_time <= 0:
            return 0.0
        return self.queue_area / effective_time

    def avg_in_service(self, effective_time: float) -> float:
        if effective_time <= 0:
            return 0.0
        return self.busy_area / effective_time
    
    def avg_in_system(self, effective_time: float) -> float:
        if effective_time <= 0:
            return 0.0
        return self.system_area / effective_time

    def utilization(self, effective_time: float) -> float:
        denom = self.servers * effective_time
        if denom <= 0:
            return 0.0
        return self.busy_area / denom
