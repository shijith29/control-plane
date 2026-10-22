"""Worker simulator CLI.

    python -m worker --fail-rate=0.2 --min-delay-ms=500 --max-delay-ms=3000

Every flag can also come from an environment variable (WORKER_FAIL_RATE, ...); flags win.
In docker compose the flags are passed through WORKER_ARGS (see README §14).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
from collections.abc import Sequence

from control_plane.logs import configure_logging
from worker.simulator import Worker, WorkerConfig


def _env(name: str, default: str) -> str:
    return os.environ.get(f"WORKER_{name}", default)


def parse_args(argv: Sequence[str] | None = None) -> tuple[str, WorkerConfig, str]:
    parser = argparse.ArgumentParser(
        prog="worker", description="Tenant provisioning worker simulator"
    )
    parser.add_argument("--rabbitmq-url", default=os.environ.get("RABBITMQ_URL"))
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=float(_env("FAIL_RATE", "0")),
        help="probability (0-1) that a task ends 'failed'",
    )
    parser.add_argument("--min-delay-ms", type=int, default=int(_env("MIN_DELAY_MS", "500")))
    parser.add_argument("--max-delay-ms", type=int, default=int(_env("MAX_DELAY_MS", "3000")))
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=float(_env("DUPLICATE_RATE", "0")),
        help="probability of publishing each update twice (same update_id)",
    )
    parser.add_argument(
        "--reorder-rate",
        type=float,
        default=float(_env("REORDER_RATE", "0")),
        help="probability of sending the terminal update before in_progress",
    )
    parser.add_argument(
        "--prefetch",
        type=int,
        default=int(_env("PREFETCH", "4")),
        help="tasks processed concurrently",
    )
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)
    if not args.rabbitmq_url:
        parser.error("--rabbitmq-url or RABBITMQ_URL is required")
    try:
        config = WorkerConfig(
            fail_rate=args.fail_rate,
            min_delay_ms=args.min_delay_ms,
            max_delay_ms=args.max_delay_ms,
            duplicate_rate=args.duplicate_rate,
            reorder_rate=args.reorder_rate,
            prefetch=args.prefetch,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args.rabbitmq_url, config, args.log_level


async def _main(url: str, config: WorkerConfig) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await Worker(url, config).run(stop)


def main(argv: Sequence[str] | None = None) -> None:
    url, config, level = parse_args(argv)
    configure_logging(level)
    asyncio.run(_main(url, config))


if __name__ == "__main__":
    main()
