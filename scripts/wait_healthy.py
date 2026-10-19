"""Wait until every started compose service is healthy (or, for one-shot jobs, exited 0).

    python3 scripts/wait_healthy.py <timeout-seconds> <start-file> -- docker compose ...

<start-file> holds the epoch second `make up` began (before `compose up`, which itself waits
for dependencies). Prints the total seconds. Stdlib only (runs on the host).
"""
import json
import subprocess
import sys
import time


def states(compose: list[str]) -> dict[str, tuple[str, str, int]]:
    out = subprocess.run([*compose, "ps", "-a", "--format", "json"], check=True,
                         capture_output=True, text=True).stdout
    rows = [json.loads(line) for line in out.splitlines() if line.strip()]
    if len(rows) == 1 and isinstance(rows[0], list):  # older compose prints one JSON array
        rows = rows[0]
    return {r["Service"]: (r["State"], r.get("Health", ""), int(r.get("ExitCode", 0))) for r in rows}


def main() -> None:
    timeout = float(sys.argv[1])
    start = float(open(sys.argv[2]).read().strip())
    compose = sys.argv[sys.argv.index("--") + 1:]
    while True:
        current = states(compose)
        failed = {s: v for s, v in current.items() if v[0] == "exited" and v[2] != 0}
        if failed:
            sys.exit(f"failed: {failed}")
        pending = {s: v for s, v in current.items()
                   # Every long-running service has a healthcheck, so running without one means
                   # a one-shot job that has not finished.
                   if not (v[0] == "exited" or (v[0] == "running" and v[1] == "healthy"))}
        if current and not pending:
            seconds = round(time.time() - start)
            print(f"stack healthy in {seconds} s")
            return
        if time.time() - start > timeout:
            sys.exit(f"timed out after {timeout:.0f} s; still waiting for: {pending}")
        time.sleep(2)


if __name__ == "__main__":
    main()
