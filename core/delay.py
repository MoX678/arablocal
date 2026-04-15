"""Adaptive delay with backoff/speedup for rate limiting."""

from __future__ import annotations

import asyncio
import random


class AdaptiveDelay:
    """Dynamic delay that backs off on failures and speeds up on successes.

    Thread-safe via asyncio.Lock. Adds jitter to avoid thundering herd.
    """

    def __init__(self, base: float = 0.6, minimum: float = 0.2, maximum: float = 10.0):
        self.current = base
        self.minimum = minimum
        self.maximum = maximum
        self._lock = asyncio.Lock()

    async def wait(self):
        """Sleep for current delay + random jitter."""
        jitter = random.uniform(0, self.current * 0.5)
        await asyncio.sleep(self.current + jitter)

    async def on_success(self):
        """Reduce delay after successful request."""
        async with self._lock:
            self.current = max(self.minimum, self.current * 0.85)

    async def on_failure(self):
        """Increase delay after failed request."""
        async with self._lock:
            self.current = min(self.maximum, self.current * 1.5)
