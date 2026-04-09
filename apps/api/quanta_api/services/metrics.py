from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from statistics import mean

import httpx

from quanta_api.core.config import Settings


@dataclass
class TimingSeries:
    count: int = 0
    total_ms: float = 0.0
    values_ms: list[float] = field(default_factory=list)

    def record(self, duration_ms: float) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.values_ms.append(duration_ms)

    def snapshot(self) -> dict[str, float]:
        if not self.values_ms:
            return {"count": 0, "avg_ms": 0.0, "max_ms": 0.0}
        return {
            "count": self.count,
            "avg_ms": round(mean(self.values_ms), 2),
            "max_ms": round(max(self.values_ms), 2),
        }


class MetricsService:
    def __init__(self, settings: Settings | None = None, client: httpx.Client | None = None) -> None:
        self._counters: Counter[str] = Counter()
        self._timings: dict[str, TimingSeries] = {}
        self._gauges: dict[str, float] = {}
        self.settings = settings
        self.client = client or httpx.Client(timeout=5.0)

    def increment(self, metric_name: str, value: int = 1) -> None:
        self._counters[metric_name] += value
        self._export_otlp()

    def record_timing(self, metric_name: str, duration_ms: float) -> None:
        series = self._timings.setdefault(metric_name, TimingSeries())
        series.record(duration_ms)
        self._export_otlp()

    def set_gauge(self, metric_name: str, value: float) -> None:
        self._gauges[metric_name] = value
        self._export_otlp()

    def snapshot(self) -> dict[str, object]:
        return {
            "counters": dict(self._counters),
            "timings": {metric_name: series.snapshot() for metric_name, series in self._timings.items()},
            "gauges": dict(self._gauges),
        }

    def render_prometheus(self) -> str:
        lines: list[str] = []
        for name, value in sorted(self._counters.items()):
            metric = self._prom_name(name)
            lines.append(f"# TYPE {metric} counter")
            lines.append(f"{metric} {value}")
        for name, series in sorted(self._timings.items()):
            metric = self._prom_name(name)
            snapshot = series.snapshot()
            lines.append(f"# TYPE {metric}_avg gauge")
            lines.append(f"{metric}_avg {snapshot['avg_ms']}")
            lines.append(f"# TYPE {metric}_max gauge")
            lines.append(f"{metric}_max {snapshot['max_ms']}")
            lines.append(f"# TYPE {metric}_count counter")
            lines.append(f"{metric}_count {snapshot['count']}")
        for name, value in sorted(self._gauges.items()):
            metric = self._prom_name(name)
            lines.append(f"# TYPE {metric} gauge")
            lines.append(f"{metric} {value}")
        return "\n".join(lines) + ("\n" if lines else "")

    def _export_otlp(self) -> None:
        if not self.settings or not self.settings.metrics_otlp_endpoint:
            return
        try:
            self.client.post(
                self.settings.metrics_otlp_endpoint,
                headers={"x-quanta-metrics-secret": self.settings.metrics_otlp_secret or ""},
                json={"resource": {"service.name": "strantas-quanta"}, "metrics": self.snapshot()},
            )
        except httpx.HTTPError:
            return

    def _prom_name(self, metric_name: str) -> str:
        return metric_name.replace(".", "_")
