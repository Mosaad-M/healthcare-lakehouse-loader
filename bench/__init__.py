"""Scale and performance benchmarks. Each step runs in its own container capped like the loader's
ECS task (2 vCPU, 8 GB): locally, or as a Fargate task in the AWS sandbox. AWS cost is the request
counts measured here times AWS prices (bench/cost.py).

    python3 bench/run.py <scenario>                    locally; results in bench/results/local/
    scripts/sandbox.py bench <scenario> [--max N]      on AWS; results in bench/results/aws/
    python3 bench/report.py [--results DIR]            summary: limits, unit costs, cost at a target
    python -m bench.step ...                           one step (inside the `bench` service)

Synthetic data only. Benchmark sources are named bench_<scenario> and are removed afterwards.
"""
