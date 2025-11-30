# src/experiments.py
from sim_engine import run_experiment
from config import config

def main():
    # change arrival rates to test workloads
    lams = [float(i) for i in range(31, 41)]
    for lam in lams:
        config['arrival_rate'] = lam
        out = run_experiment(run_time=2000.0, warmup_time=200.0, replications=3, base_seed=1000, output_dir=f"outputs/results_csv/workload_lambda_{lam}", workload_name=f"lam_{lam}")
        print("Saved summary:", out['summary_file'])

if __name__ == "__main__":
    main()
