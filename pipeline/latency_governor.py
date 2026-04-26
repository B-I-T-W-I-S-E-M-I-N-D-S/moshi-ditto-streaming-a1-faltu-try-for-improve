"""
pipeline/latency_governor.py
============================
Adaptive latency controller for the streaming pipeline.
"""

from __future__ import annotations

import enum
import logging
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


class LatencyMode(str, enum.Enum):
    NORMAL = "normal"
    FAST = "fast"
    EMERGENCY = "emergency"


@dataclass(frozen=True)
class QueueLimits:
    token_q: int
    frame_q: int
    send_q: int


@dataclass(frozen=True)
class LatencyThresholds:
    fast_pressure: float = 0.60
    emergency_pressure: float = 0.85
    recover_pressure: float = 0.35
    mode_hold_s: float = 1.0


@dataclass(frozen=True)
class FreshnessBudgetMs:
    token: float = 220.0
    frame: float = 140.0
    send: float = 120.0


@dataclass(frozen=True)
class RuntimeProfile:
    sampling_timesteps: int
    overlap_v2: int
    jpeg_quality: int
    max_video_fps: float
    bridge_chunk: int
    bridge_flush_timeout_ms: int


def profile_for(name: str) -> RuntimeProfile:
    key = (name or "normal").strip().lower()
    if key == "emergency":
        return RuntimeProfile(6, 78, 50, 15.0, 1, 20)
    if key == "fast":
        return RuntimeProfile(8, 77, 60, 20.0, 1, 30)
    return RuntimeProfile(10, 76, 70, 25.0, 1, 50)


class LatencyGovernor:
    def __init__(
        self,
        queue_limits: QueueLimits,
        thresholds: Optional[LatencyThresholds] = None,
    ):
        self.queue_limits = queue_limits
        self.thresholds = thresholds or LatencyThresholds()
        self._mode: LatencyMode = LatencyMode.NORMAL
        self._mode_since = time.monotonic()

    @property
    def mode(self) -> LatencyMode:
        return self._mode

    def _pressure(self, token_q: int, frame_q: int, send_q: int) -> float:
        p1 = token_q / max(1, self.queue_limits.token_q)
        p2 = frame_q / max(1, self.queue_limits.frame_q)
        p3 = send_q / max(1, self.queue_limits.send_q)
        return max(p1, p2, p3)

    def update(self, token_q: int, frame_q: int, send_q: int) -> LatencyMode:
        now = time.monotonic()
        pressure = self._pressure(token_q, frame_q, send_q)
        if now - self._mode_since < self.thresholds.mode_hold_s:
            return self._mode
        prev = self._mode
        if pressure >= self.thresholds.emergency_pressure:
            self._mode = LatencyMode.EMERGENCY
        elif pressure >= self.thresholds.fast_pressure:
            self._mode = LatencyMode.FAST
        elif pressure <= self.thresholds.recover_pressure:
            self._mode = LatencyMode.NORMAL
        if self._mode != prev:
            self._mode_since = now
            logger.warning(
                "[LatencyGovernor] mode %s -> %s (pressure=%.2f)",
                prev.value, self._mode.value, pressure,
            )
        return self._mode

    def bridge_throttle_s(self) -> float:
        if self._mode == LatencyMode.EMERGENCY:
            return 0.02
        if self._mode == LatencyMode.FAST:
            return 0.008
        return 0.0

