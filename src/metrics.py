# src/metrics.py
import csv
import math
from typing import Dict, List
import statistics

class Metrics:
    def __init__(self, nodes: Dict[str, object], warmup_time: float, run_time: float, output_dir: str = "outputs/results_csv"):
        """
        nodes: dict of name -> QueueNode
        warmup_time: float (time to ignore/transient)
        run_time: float (collection time after warmup)
        """
        self.nodes = nodes
        self.warmup_time = warmup_time
        self.run_time = run_time
        self.effective_time = max(0.0, run_time)  # we will assume nodes.finalize called with warmup+run_time
        self.patients = []  # store Patient objects (all created)
        self.output_dir = output_dir

    def add_patient(self, patient):
        self.patients.append(patient)

    def _patients_after_warmup(self):
        # include patients whose registration arrival >= warmup_time
        return [p for p in self.patients if (p.get('registration_arrival') is not None and p.get('registration_arrival') >= self.warmup_time)]

    def finalize_nodes(self, sim_end_time: float):
        # call finalize on nodes so they accumulate areas up to sim_end_time
        for node in self.nodes.values():
            try:
                node.finalize(sim_end_time)
            except Exception:
                pass

    def compute_node_metrics(self):
        """
        Compute per-node metrics: mean_wait, mean_service, mean_response (per-node),
        avg_queue_length, avg_in_service, utilization, num_completed_jobs
        """
        node_stats = {}
        effective_T = self.effective_time if self.effective_time > 0 else 1.0
        for name, node in self.nodes.items():
            # compute average waiting time and service time based on patients who visited
            waits = []
            services = []
            responses = []
            for p in self._patients_after_warmup():
                # waiting at node = service_start - arrival at node
                a = p.get(f"{name}_arrival")
                s = p.get(f"{name}_service_start")
                e = p.get(f"{name}_service_end")
                if a is not None and s is not None:
                    waits.append(max(0.0, s - a))
                if s is not None and e is not None:
                    services.append(max(0.0, e - s))
                if a is not None and e is not None:
                    responses.append(max(0.0, e - a))
            mean_wait = statistics.mean(waits) if waits else 0.0
            mean_service = statistics.mean(services) if services else 0.0
            mean_response = statistics.mean(responses) if responses else 0.0

            avg_q = node.avg_queue_length(effective_T)
            avg_in_service = node.avg_in_service(effective_T)
            avg_in_system = node.avg_in_system(effective_T)

            util = node.utilization(effective_T)
            node_stats[name] = {
                'mean_waiting_time': mean_wait,
                'mean_service_time': mean_service,
                'mean_response_time': mean_response,
                'avg_queue_length_timeavg': avg_q,
                'avg_in_service_timeavg': avg_in_service,
                'avg_in_system': avg_in_system,
                'utilization': util,
                'num_completed_jobs': node.completed_jobs
            }
        return node_stats

    # def compute_overall_metrics(self):
    #     # E[w], E[R] overall (across nodes/patients)
    #     waits = []
    #     responses = []
    #     for p in self._patients_after_warmup():
    #         # accumulate waiting times across nodes visited
    #         for node in ['registration', 'doctor', 'lab', 'pharmacy']:
    #             a = p.get(f"{node}_arrival")
    #             s = p.get(f"{node}_service_start")
    #             if a is not None and s is not None:
    #                 waits.append(max(0.0, s - a))
    #         # response total: patient.exit_time() - arrival_time (if exit exists)
    #         exit_t = p.exit_time()
    #         if exit_t is not None:
    #             responses.append(max(0.0, exit_t - p.arrival_time))
    #     Ew = statistics.mean(waits) if waits else 0.0
    #     Er = statistics.mean(responses) if responses else 0.0
    #     return {'E[w]': Ew, 'E[R]': Er, 'num_patients': len(self._patients_after_warmup())}
    
    def compute_overall_metrics(self):
        # E[w], E[R] overall (across nodes/patients)
        waits = []
        responses = []
        for p in self._patients_after_warmup():
            # accumulate waiting times across nodes visited
            total = 0
            for node in ['registration', 'doctor', 'lab', 'pharmacy']:
                a = p.get(f"{node}_arrival")
                s = p.get(f"{node}_service_start")
                if a is not None and s is not None:
                    total+= s-a
            waits.append(total)
            # response total: patient.exit_time() - arrival_time (if exit exists)
            exit_t = p.exit_time()
            if exit_t is not None:
                responses.append(max(0.0, exit_t - p.arrival_time))
        Ew = statistics.mean(waits) if waits else 0.0
        Er = statistics.mean(responses) if responses else 0.0
        return {'E[w]': Ew, 'E[R]': Er, 'num_patients': len(self._patients_after_warmup())}

    def write_per_patient_csv(self, filepath: str, workload: str, rep: int, seed: int):
        header = [
            'workload', 'rep', 'seed', 'patient_id', 'arrival_time',
            'reg_arrival', 'reg_service_start', 'reg_service_end',
            'doc_arrival', 'doc_service_start', 'doc_service_end',
            'lab_arrival', 'lab_service_start', 'lab_service_end',
            'phar_arrival', 'phar_service_start', 'phar_service_end',
            'exit_time'
        ]
        rows = []
        for p in self._patients_after_warmup():
            row = [
                workload, rep, seed, p.id, p.arrival_time,
                p.get('registration_arrival'), p.get('registration_service_start'), p.get('registration_service_end'),
                p.get('doctor_arrival'), p.get('doctor_service_start'), p.get('doctor_service_end'),
                p.get('lab_arrival'), p.get('lab_service_start'), p.get('lab_service_end'),
                p.get('pharmacy_arrival'), p.get('pharmacy_service_start'), p.get('pharmacy_service_end'),
                p.exit_time()
            ]
            rows.append(row)
        # write csv
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)

    def write_per_node_csv(self, filepath: str, workload: str, rep: int, seed: int):
        node_stats = self.compute_node_metrics()
        header = [
            'workload', 'rep', 'seed', 'node_name', 'servers', 'mu', 'lambda_effective',
            'E[w]', 'E[s]', 'E[r]',
            'E[n_q]', 'E[n_s]', 'E[n]', 'utilization', 'num_completed_jobs'
        ]
        rows = []
        # estimate lambda_effective using visit ratios: here we assume external lambda belongs to config usage (caller)
        for name, s in node_stats.items():
            row = [
                workload, rep, seed, name,
                self.nodes[name].servers,
                self.nodes[name].service_rate,
                None,  # lambda_effective to be filled by caller if desired
                s['mean_waiting_time'],
                s['mean_service_time'],
                s['mean_response_time'],
                s['avg_queue_length_timeavg'],
                s['avg_in_service_timeavg'],
                s['avg_in_system'],
                s['utilization'],
                s['num_completed_jobs']
            ]
            rows.append(row)
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
