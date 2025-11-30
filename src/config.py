# src/config.py
# Cấu hình mặc định cho mô phỏng. Thay đổi ở đây để thử nghiệm.

config = {
    'arrival_rate': 5.0,    # lambda: arrivals per time unit
    'routing': {
        'p_lab': 0.2        # probability patient goes to lab after doctor
    },
    'nodes': {
        # name: { service_rate (mu), servers (c) }
        'registration': {'service_rate': 8.0, 'servers': 3},
        'doctor':       {'service_rate': 5.0, 'servers': 5},
        'lab':          {'service_rate': 10.0, 'servers': 4},
        'pharmacy':     {'service_rate': 6.0, 'servers': 2}
    },
    # default experiment meta (used by experiments.py if desired)
    'default_run_time': 20000.0,
    'default_warmup_time': 2000.0,
    'default_replications': 10
}
