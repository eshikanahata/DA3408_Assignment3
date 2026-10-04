import argparse
import signal
import sys
import time

from ray.cluster_utils import Cluster

p = argparse.ArgumentParser()
p.add_argument("--cpus", type=int, default=2, help="CPUs per worker node")
p.add_argument("--object-store-gb", type=float, default=2.0, help="object store per worker node")
args = p.parse_args()

cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0, "include_dashboard": True})
for _ in range(2):
    cluster.add_node(num_cpus=args.cpus, object_store_memory=int(args.object_store_gb * 1e9))
cluster.wait_for_nodes()

with open(".ray_address", "w") as f:
    f.write(cluster.address)
print("Ray dashboard: http://127.0.0.1:8265", flush=True)
print(f"Ray cluster up at {cluster.address} with 2 workers x {args.cpus} CPUs", flush=True)

signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
try:
    while True:
        time.sleep(1)
finally:
    cluster.shutdown()
