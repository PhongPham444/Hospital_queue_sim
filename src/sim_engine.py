# src/sim_engine.py
import simpy
import random
import argparse
import os
import csv
from config import config
from queue_node import QueueNode
from arrival import arrival_generator
from router import route_after_doctor
from metrics import Metrics

def run_once(run_time, warmup_time, seed, out_dir, workload_name="default"):
    random.seed(seed)
    env = simpy.Environment()
    # create nodes
    nodes = {}
    for name, params in config['nodes'].items():
        nodes[name] = QueueNode(env, name, params['service_rate'], params['servers'])

    # metrics instance: effective run_time is run_time (we will run env until warmup+run_time)
    metrics = Metrics(nodes, warmup_time, run_time, output_dir=out_dir)

    # spawn arrival process (lambda is config['arrival_rate'])
    env.process(arrival_generator(env, config['arrival_rate'], nodes['registration'], metrics))

    # We need to ensure that after finishing registration, patient proceeds to doctor,
    # and after doctor, router drives them to lab and pharmacy.
    # Easiest approach: wrap registration.serve to chain processes by modifying QueueNode.serve usage:
    # For simplicity: we will create a small helper that periodically checks for patients who have finished registration
    # but this is complex. Instead, we will embed chaining by making registration.serve call doctor and router
    # To avoid modifying QueueNode class, we will monkey-patch registration.serve with a wrapper function.

    # Save original serve
    original_reg_serve = nodes['registration'].serve

    def registration_serve_wrapper(patient):
        # call original registration serve; after it completes, chain doctor and routing
        yield from original_reg_serve(patient)
        # immediately go to doctor
        yield from nodes['doctor'].serve(patient)
        # after doctor, route to lab/pharmacy
        yield from route_after_doctor(env, patient, nodes['lab'], nodes['pharmacy'], config['routing']['p_lab'])

    # monkey-patch
    nodes['registration'].serve = registration_serve_wrapper

    sim_end_time = warmup_time + run_time
    env.run(until=sim_end_time)

    # finalize node integrals up to sim_end_time
    metrics.finalize_nodes(sim_end_time)

    return metrics

def run_experiment(run_time, warmup_time, replications, base_seed, output_dir, workload_name="default"):
    os.makedirs(output_dir, exist_ok=True)
    # per_patient_dir = os.path.join(output_dir, "per_patient")
    per_node_dir = os.path.join(output_dir, "per_node_rep")
    summary_dir = os.path.join(output_dir, "summaries")
    # os.makedirs(per_patient_dir, exist_ok=True)
    os.makedirs(per_node_dir, exist_ok=True)
    os.makedirs(summary_dir, exist_ok=True)

    all_node_stats = []  # collect per-rep node stats for summary
    all_overall_stats = []  # collect per-rep overall stats (E[w], E[R], num_patients)

    for rep in range(replications):
        seed = base_seed + rep
        metrics = run_once(run_time, warmup_time, seed, output_dir, workload_name)
        sim_end = warmup_time + run_time
        # finalize nodes to account area up to sim_end
        metrics.finalize_nodes(sim_end)

        # write per-patient CSV
        # per_patient_file = os.path.join(per_patient_dir, f"{workload_name}_rep{rep}_seed{seed}.csv")
        # metrics.write_per_patient_csv(per_patient_file, workload_name, rep, seed)

        # write per-node CSV
        per_node_file = os.path.join(per_node_dir, f"{workload_name}_rep{rep}_seed{seed}.csv")
        metrics.write_per_node_csv(per_node_file, workload_name, rep, seed)

        # compute stats
        node_stats = metrics.compute_node_metrics()
        overall = metrics.compute_overall_metrics()
        all_node_stats.append((rep, seed, node_stats))
        all_overall_stats.append((rep, seed, overall))

    # produce a simple summary CSV for overall metrics
    summary_file = os.path.join(summary_dir, f"{workload_name}_summary.csv")
    # aggregate across reps
    # compute mean and std for E[w] and E[R]
    # Ew_vals = [o['E[w]'] for (_, _, o) in [(r,s,o) for (r,s,o) in [(x[0], x[1], x[2]) for x in all_overall_stats]]]
    # Er_vals = [o['E[R]'] for (_, _, o) in [(r,s,o) for (r,s,o) in [(x[0], x[1], x[2]) for x in all_overall_stats]]]

    # simpler: compute via collected all_overall_stats list
    Ew_list = [entry[2]['E[w]'] for entry in all_overall_stats]
    Er_list = [entry[2]['E[R]'] for entry in all_overall_stats]

    import statistics, math

    def mean_std_ci(xlist):
        if not xlist:
            return (0.0, 0.0, 0.0, 0.0)
        m = statistics.mean(xlist)
        s = statistics.stdev(xlist) if len(xlist) > 1 else 0.0
        n = len(xlist)
        # 95% t-interval approx using t_{0.975, n-1} ~ 2. block if n small
        t = 2.0 if n > 1 else 0.0
        ci = t * s / math.sqrt(n) if n > 1 else 0.0
        return (m, s, m - ci, m + ci)

    Ew_m, Ew_s, Ew_lo, Ew_hi = mean_std_ci(Ew_list)
    Er_m, Er_s, Er_lo, Er_hi = mean_std_ci(Er_list)

    with open(summary_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'mean', 'std', 'ci_low', 'ci_high', 'n_rep'])
        writer.writerow(['E[w]', Ew_m, Ew_s, Ew_lo, Ew_hi, replications])
        writer.writerow(['E[R]', Er_m, Er_s, Er_lo, Er_hi, replications])

    return {
        'Ew_list': Ew_list,
        'Er_list': Er_list,
        'summary_file': summary_file,
        # 'per_patient_dir': per_patient_dir,
        'per_node_dir': per_node_dir
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_time", type=float, default=config['default_run_time'])
    parser.add_argument("--warmup_time", type=float, default=config['default_warmup_time'])
    parser.add_argument("--replications", type=int, default=config['default_replications'])
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument("--output", type=str, default="outputs/results_csv")
    parser.add_argument("--workload", type=str, default="default")
    args = parser.parse_args()

    res = run_experiment(args.run_time, args.warmup_time, args.replications, args.seed, args.output, args.workload)
    print("Experiment finished. Summary:", res['summary_file'])
