"""Exponential backoff with full jitter, shared by the relay, the consumer and the worker."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class Backoff:
    base: float = 0.1
    cap: float = 30.0
    factor: float = 2.0
    _attempt: int = field(default=0, init=False)

    def next_delay(self) -> float:
        """Delay for the next retry: uniform in [0, min(cap, base * factor**attempt)]."""
        ceiling = min(self.cap, self.base * (self.factor**self._attempt))
        self._attempt += 1
        # jitter only, not cryptography
        return random.uniform(0, ceiling)  # noqa: S311  # nosec B311

    def reset(self) -> None:
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt
