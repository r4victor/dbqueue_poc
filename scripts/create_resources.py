#!/usr/bin/env python3
import argparse
import json
import time
from itertools import cycle
from urllib import request


def _post_json(base_url: str, path: str, payload: dict, timeout: float) -> dict:
    url = f"{base_url.rstrip('/')}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("Value must be >= 0")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create runs, jobs, and placement groups via API.",
    )
    parser.add_argument("n1_runs", type=_non_negative_int, help="How many runs to create")
    parser.add_argument("n2_jobs", type=_non_negative_int, help="How many jobs to create")
    parser.add_argument(
        "n3_placement_groups",
        type=_non_negative_int,
        help="How many placement groups to create",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="API base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP request timeout in seconds (default: %(default)s)",
    )
    args = parser.parse_args()

    if args.n2_jobs > 0 and args.n1_runs == 0:
        parser.error("n2_jobs > 0 requires n1_runs > 0")

    suffix = int(time.time())
    run_ids: list[str] = []

    for i in range(args.n1_runs):
        run_response = _post_json(
            args.base_url,
            "/runs/create",
            {"name": f"seed-run-{suffix}-{i}"},
            timeout=args.timeout,
        )
        run_ids.append(run_response["id"])

    if args.n2_jobs > 0:
        run_id_cycle = cycle(run_ids)
        for i in range(args.n2_jobs):
            _post_json(
                args.base_url,
                "/jobs/create",
                {"run_id": next(run_id_cycle), "name": f"seed-job-{suffix}-{i}"},
                timeout=args.timeout,
            )

    for i in range(args.n3_placement_groups):
        _post_json(
            args.base_url,
            "/placement-groups/create",
            {"name": f"seed-pg-{suffix}-{i}"},
            timeout=args.timeout,
        )

    print(
        "Created resources:"
        f" runs={args.n1_runs}, jobs={args.n2_jobs}, placement_groups={args.n3_placement_groups}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
