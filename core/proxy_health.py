"""Live proxy health tracking with quarantine and exponential backoff."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

log = logging.getLogger("arablocal")

# Quarantine backoff schedule (seconds): 30s → 60s → 120s → 300s → permanent
_BACKOFF_SCHEDULE = [30, 60, 120, 300]


@dataclass
class ProxyRecord:
    """Health stats for a single proxy."""
    address: str
    successes: int = 0
    failures: int = 0
    total_latency: float = 0.0
    quarantine_until: float = 0.0   # time.monotonic() when quarantine lifts
    quarantine_count: int = 0       # how many times quarantined
    last_used: float = 0.0
    last_error: str = ""

    @property
    def avg_latency(self) -> float:
        return (self.total_latency / self.successes) if self.successes else 0.0

    @property
    def success_rate(self) -> float:
        total = self.successes + self.failures
        return (self.successes / total * 100) if total else 100.0

    @property
    def is_quarantined(self) -> bool:
        return time.monotonic() < self.quarantine_until

    @property
    def is_dead(self) -> bool:
        """Permanently dead after exhausting backoff schedule."""
        return self.quarantine_count > len(_BACKOFF_SCHEDULE)


class ProxyHealthMonitor:
    """Track proxy health, quarantine failing proxies, pick healthy ones.

    Thread-safe via external locking (called from async engine).
    """

    def __init__(self, proxies: List[str], min_pool_size: int = 2):
        self._records: Dict[str, ProxyRecord] = {}
        self._min_pool_size = min_pool_size
        for p in proxies:
            self._records[p] = ProxyRecord(address=p)

    @property
    def all_records(self) -> List[ProxyRecord]:
        return list(self._records.values())

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self._records.values() if not r.is_quarantined and not r.is_dead)

    @property
    def quarantined_count(self) -> int:
        return sum(1 for r in self._records.values() if r.is_quarantined)

    @property
    def dead_count(self) -> int:
        return sum(1 for r in self._records.values() if r.is_dead)

    def record_success(self, proxy: str, latency: float):
        """Record a successful request through this proxy."""
        rec = self._records.get(proxy)
        if not rec:
            return
        rec.successes += 1
        rec.total_latency += latency
        rec.last_used = time.monotonic()
        # Success resets quarantine progression
        if rec.quarantine_count > 0:
            rec.quarantine_count = max(0, rec.quarantine_count - 1)

    def record_failure(self, proxy: str, error: str = "", is_site_fault: bool = False):
        """Record a failed request. Only quarantine on proxy faults.

        Args:
            proxy: The proxy address.
            error: Error description.
            is_site_fault: True if the error is from the target site (4xx, CF block),
                           not the proxy itself. Site faults don't penalize the proxy.
        """
        rec = self._records.get(proxy)
        if not rec:
            return
        rec.last_used = time.monotonic()
        rec.last_error = error

        if is_site_fault:
            return  # Don't blame the proxy

        rec.failures += 1

        # Quarantine after 3 consecutive failures (check recent pattern)
        recent_fail_rate = rec.failures / max(rec.successes + rec.failures, 1)
        if recent_fail_rate > 0.6 and rec.failures >= 3:
            self._quarantine(rec)

    def _quarantine(self, rec: ProxyRecord):
        """Put proxy in quarantine with exponential backoff."""
        # Don't quarantine if it would drop pool below minimum
        if self.healthy_count <= self._min_pool_size:
            log.warning(
                f"[proxy-health] Would quarantine {rec.address} but pool at minimum "
                f"({self.healthy_count}/{self._min_pool_size})"
            )
            return

        idx = min(rec.quarantine_count, len(_BACKOFF_SCHEDULE) - 1)
        backoff = _BACKOFF_SCHEDULE[idx]
        rec.quarantine_until = time.monotonic() + backoff
        rec.quarantine_count += 1

        if rec.is_dead:
            log.warning(f"[proxy-health] {rec.address} marked DEAD after {rec.quarantine_count} quarantines")
        else:
            log.info(f"[proxy-health] {rec.address} quarantined for {backoff}s (round {rec.quarantine_count})")

    def pick_proxy(self) -> Optional[str]:
        """Pick the best available proxy (lowest latency among healthy ones)."""
        now = time.monotonic()
        available = [
            r for r in self._records.values()
            if not r.is_dead and now >= r.quarantine_until
        ]
        if not available:
            # Emergency: un-quarantine the least-bad proxy
            all_alive = [r for r in self._records.values() if not r.is_dead]
            if all_alive:
                best = min(all_alive, key=lambda r: r.quarantine_until)
                best.quarantine_until = 0
                log.warning(f"[proxy-health] Emergency un-quarantine: {best.address}")
                return best.address
            return None

        # Weight by success rate; add jitter to avoid hotspot
        available.sort(key=lambda r: (-r.success_rate, r.avg_latency))
        # Pick from top 3 randomly for load distribution
        top = available[:min(3, len(available))]
        return random.choice(top).address

    def get_summary(self) -> dict:
        """Get summary for GUI display."""
        return {
            "total": len(self._records),
            "healthy": self.healthy_count,
            "quarantined": self.quarantined_count,
            "dead": self.dead_count,
            "proxies": [
                {
                    "address": r.address,
                    "successes": r.successes,
                    "failures": r.failures,
                    "avg_latency": round(r.avg_latency, 2),
                    "success_rate": round(r.success_rate, 1),
                    "status": "dead" if r.is_dead else "quarantined" if r.is_quarantined else "healthy",
                    "last_error": r.last_error,
                }
                for r in self._records.values()
            ],
        }
