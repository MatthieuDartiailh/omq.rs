#!/usr/bin/env python3
"""Generate comparison SVG charts from benchmarks/comparisons.jsonl.

Produces:
  doc/charts/pushpull/tcp.svg  - TCP throughput + CPU%
"""

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"
COMPARISON_CHART_SIZES = {16, 64, 256, 1024, 4096, 16384}
SMALL_MESSAGE_SIZES = [16, 64, 256, 1024]
LARGE_MESSAGE_SIZES = [256, 1024, 4096, 16384]
METRIC_LINE_WIDTH = 2.5
CPU_LINE_WIDTH = 1.6
MARKER_RADIUS = 3.0
MSG_LINE_DASH = "6,3"
CPU_LINE_DASH = "2,5"

COLORS = {
    "libzmq": "#eab308",
    "libzmq-mt": "#a16207",
    "omq-tokio": "#f97316",
    "omq-tokio-mt": "#dc2626",
    "zmq.rs": "#2563eb",
    "rzmq": "#16a34a",
    "rzmq-iouring": "#15803d",
    "omq-libzmq": "#06b6d4",
}

LABELS = {
    "libzmq": "libzmq v4.3.5 (1T)",
    "libzmq-mt": "libzmq v4.3.5 (4T)",
    "omq-tokio": "omq-tokio (1T)",
    "omq-tokio-mt": "omq-tokio (4T)",
    "zmq.rs": "zmq.rs v0.6.0 [6T]",
    "rzmq": "rzmq v0.5.24 [6T]",
    "rzmq-iouring": "rzmq v0.5.24 (io_uring) [6T]",
    "omq-libzmq": "omq-libzmq [1T]",
}


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024 * 1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


# ── data loading ──────────────────────────────────────────────────

def load_jsonl() -> list[dict]:
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found", file=sys.stderr)
        sys.exit(1)
    rows = []
    for seq, line in enumerate(JSONL_PATH.read_text().splitlines()):
        line = line.strip()
        if line:
            try:
                row = json.loads(line)
                if str(row.get("run_id", "")).startswith("debug-"):
                    continue
                row["_seq"] = seq
                rows.append(row)
            except json.JSONDecodeError:
                continue
    return rows


def load_data(transport: str, impls: list[str]) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows if r.get("transport") == transport]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    lat: dict[int, dict[str, float]] = {}
    lat_cpu: dict[int, dict[str, float]] = {}

    seen_tput: dict[tuple, int] = {}
    seen_lat: dict[tuple, int] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        seq = r.get("_seq", 0)
        size = r.get("msg_size")
        kind = r.get("kind")

        if kind == "throughput":
            key = (impl_name, size)
            if key not in seen_tput or seq >= seen_tput[key]:
                seen_tput[key] = seq
                msgs_s = r.get("msgs_s", 0)
                mbps = r.get("mbps", 0)
                gbs = mbps / 1000.0
                tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)
                cpu_time = r.get("push_cpu_time", r.get("cpu_time", 0))
                elapsed = r.get("elapsed", 0)
                if elapsed > 0 and cpu_time > 0:
                    tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

        elif kind == "latency":
            key = (impl_name, size)
            if key not in seen_lat or seq >= seen_lat[key]:
                seen_lat[key] = seq
                lat.setdefault(size, {})[impl_name] = r.get("p50_us", 0)
                cpu_time = r.get("req_cpu_time", r.get("cpu_time", 0))
                elapsed = r.get("elapsed", 0)
                if elapsed > 0 and cpu_time > 0:
                    lat_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s in COMPARISON_CHART_SIZES)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu,
            "lat": lat, "lat_cpu": lat_cpu}


# ── SVG helpers ───────────────────────────────────────────────────

def svg_line(x1, y1, x2, y2, stroke="#e5e7eb", width=1, dash=None) -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'  <line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}"'
        f' stroke="{stroke}" stroke-width="{width}"{d}/>'
    )


def svg_text(x, y, text, anchor="middle", fill="#374151", size=10, weight=None,
             baseline=None, rotate=None) -> str:
    parts = [f'  <text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}"']
    if baseline:
        parts[0] += f' dominant-baseline="{baseline}"'
    parts[0] += f' fill="{fill}" font-size="{size}"'
    if weight:
        parts[0] += f' font-weight="{weight}"'
    if rotate:
        parts[0] += f' transform="rotate({rotate},{x:.1f},{y:.1f})"'
    parts[0] += f">{text}</text>"
    return parts[0]


def svg_polyline(points: list[tuple[float, float]], color: str, width=2.5,
                 dash=None, opacity: float | None = None) -> str:
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    d = f' stroke-dasharray="{dash}"' if dash else ""
    o = f' opacity="{opacity:g}"' if opacity is not None else ""
    cap = ' stroke-linecap="round" stroke-linejoin="round"' if not dash else ""
    return (
        f'  <polyline points="{pts}" fill="none" stroke="{color}"'
        f' stroke-width="{width}"{cap}{d}{o}/>'
    )


def svg_dots(points: list[tuple[float, float]], color: str, radius: float = 3) -> list[str]:
    return [
        f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:g}"'
        f' fill="{color}" stroke="white" stroke-width="1"/>'
        for x, y in points
    ]


def svg_x_marks(points: list[tuple[float, float]], color: str, radius: float = 3) -> list[str]:
    return [
        f'  <path d="M {x - radius:.1f},{y - radius:.1f}'
        f' L {x + radius:.1f},{y + radius:.1f}'
        f' M {x + radius:.1f},{y - radius:.1f}'
        f' L {x - radius:.1f},{y + radius:.1f}"'
        f' stroke="{color}" stroke-width="1.6" stroke-linecap="round" fill="none"/>'
        for x, y in points
    ]


# ── chart panels ─────────────────────────────────────────────────

def draw_throughput_panel(
    L: list[str], sizes: list[int], xs: list[float], tput: dict,
    impls: list[str], x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str, log_gbs: bool = False,
    fixed_msg_max: float | None = None,
    fixed_gbs_max: float | None = None,
    msg_break: tuple[float, float] | None = None,
):
    import math

    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_msgs = [
        tput[s][name][0]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    msg_max = fixed_msg_max if fixed_msg_max else (max(all_msgs) * 1.15 if all_msgs else 16e6)
    msg_max = max(msg_max, 1.0)

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    gbs_max = max(all_gbs) if all_gbs else 10.0
    gbs_min = min(all_gbs) if all_gbs else 0.01
    if log_gbs:
        gbs_min = max(gbs_min, 0.01)
        log_lo = math.floor(math.log10(gbs_min * 0.8))
        log_ref = max(fixed_gbs_max or gbs_max, 0.01)
        log_hi = math.ceil(math.log10(log_ref * 1.15))
    else:
        tput_max = fixed_gbs_max if fixed_gbs_max else gbs_max * 1.15
        tput_max = max(tput_max, 0.001)

    if msg_break:
        break_val, bottom_frac = msg_break
        y_break = y_bot - bottom_frac * h

        def y_msg(v):
            if v <= break_val:
                return y_bot - (v / break_val) * bottom_frac * h
            return y_break - ((v - break_val) / (msg_max - break_val)) * (1 - bottom_frac) * h
    else:
        def y_msg(v):
            return y_bot - (v / msg_max) * h

    def y_tput(v):
        if log_gbs:
            if v <= 0:
                return y_bot
            frac = (math.log10(v) - log_lo) / (log_hi - log_lo)
            return y_bot - frac * h
        return y_bot - (v / tput_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # msg/s gridlines (left axis)
    if msg_break:
        _bv, _ = msg_break
        step_lo = nice_step(_bv, 4)
        v = step_lo
        while v < _bv:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            label = f"{v / 1e3:.0f}k" if v < 1e6 else f"{int(v / 1e6)}M"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_lo
        step_hi = nice_step(msg_max - _bv, 10)
        v = math.ceil(_bv / step_hi) * step_hi
        while v <= msg_max:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            millions = v / 1e6
            if millions >= 1 and millions == int(millions):
                label = f"{int(millions)}M"
            elif v >= 1e6:
                label = f"{millions:.1f}M"
            else:
                label = f"{v / 1e3:.0f}k"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_hi
    else:
        step_msg = nice_step(msg_max, 12)
        v = step_msg
        while v <= msg_max:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            millions = v / 1e6
            if millions >= 1 and millions == int(millions):
                label = f"{int(millions)}M"
            elif v >= 1e6:
                label = f"{millions:.1f}M"
            else:
                label = f"{v / 1e3:.0f}k"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_msg

    # GB/s gridlines (right axis, dashed)
    if log_gbs:
        for decade in range(log_lo, log_hi + 1):
            base = 10 ** decade
            for mult in [1, 2, 5]:
                v = base * mult
                if v < 10 ** log_lo or v > 10 ** log_hi:
                    continue
                yy = y_tput(v)
                if mult == 1:
                    L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
                    label = f"{v:.0f}" if v >= 1 else f"{v:g}"
                    L.append(svg_text(x_right + 8, yy, f"{label} GB/s",
                                      anchor="start", baseline="middle",
                                      fill="#6b7280"))
                else:
                    L.append(svg_line(x_left, yy, x_right, yy,
                                      dash="2,8", stroke="#e5e7eb"))
    else:
        step_gbs = nice_step(tput_max, 5)
        v = step_gbs
        while v <= tput_max:
            yy = y_tput(v)
            L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
            L.append(svg_text(x_right + 8, yy, f"{v:.0f} GB/s",
                              anchor="start", baseline="middle", fill="#6b7280"))
            v += step_gbs

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    if msg_break:
        _, _bf = msg_break
        yb = y_bot - _bf * h
        gap = 6
        L.append(
            f'  <rect x="{x_left - 1:.1f}" y="{yb - gap:.1f}"'
            f' width="3" height="{2 * gap}" fill="white"/>'
        )
        L.append(
            f'  <path d="M {x_left - 5:.1f},{yb + gap:.1f}'
            f' L {x_left + 5:.1f},{yb + 1:.1f}'
            f' M {x_left - 5:.1f},{yb - 1:.1f}'
            f' L {x_left + 5:.1f},{yb - gap:.1f}"'
            f' stroke="#9ca3af" stroke-width="1.5" fill="none"/>'
        )

    # axis labels
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "msg/s", weight="600", rotate=-90))

    # dashed msg/s lines
    draw_order = [name for name in
                  ["rzmq-iouring", "rzmq", "zmq.rs", "libzmq", "libzmq-mt",
                   "omq-tokio-mt", "omq-tokio"]
                  if name in impls]
    for name in draw_order:
        pts = [
            (xs[i], y_msg(tput[sizes[i]][name][0]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2, dash="6,4"))

    # solid throughput lines
    for name in draw_order:
        pts = [
            (xs[i], y_tput(tput[sizes[i]][name][1]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=1.5))
            zero_pts = []
            nonzero_pts = []
            for i in range(len(sizes)):
                if name not in tput.get(sizes[i], {}):
                    continue
                pt = (xs[i], y_tput(tput[sizes[i]][name][1]))
                if tput[sizes[i]][name][0] == 0:
                    zero_pts.append(pt)
                else:
                    nonzero_pts.append(pt)
            L.extend(svg_dots(nonzero_pts, COLORS[name], radius=2.2))
            L.extend(svg_x_marks(zero_pts, COLORS[name], radius=3))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def draw_latency_panel(
    L: list[str], sizes: list[int], xs: list[float], lat: dict,
    impls: list[str], x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str, fixed_lat_max: float | None = None,
):
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_vals = [
        lat[s][name]
        for s in sizes for name in impls if name in lat.get(s, {})
    ]
    lat_max = fixed_lat_max if fixed_lat_max else (max(all_vals) * 1.2 if all_vals else 150.0)

    def y_lat(v):
        return y_bot - (v / lat_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # gridlines
    step = nice_step(lat_max, 10)
    v = step
    while v <= lat_max:
        yy = y_lat(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{v:.0f}", anchor="end", baseline="middle"))
        v += step

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    # axis label
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "p50 latency (µs)", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["rzmq-iouring", "libzmq", "libzmq-mt", "omq-tokio-mt", "omq-tokio",
                   "rzmq", "zmq.rs"]
                  if name in impls]
    for name in draw_order:
        pts = [
            (xs[i], y_lat(lat[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=1.5))
            L.extend(svg_dots(pts, COLORS[name], radius=2.2))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def _cpu_ticks(data_max):
    """Return (axis_max, tick_values) for a linear 0-based CPU% axis."""
    if data_max <= 0:
        data_max = 100
    candidates = [50, 100, 200, 400, 500, 800, 1000]
    ceil = data_max
    for c in candidates:
        if c >= data_max:
            ceil = c
            break
    else:
        import math
        ceil = math.ceil(data_max / 100) * 100
    step = 50 if ceil <= 400 else 100
    ticks = list(range(step, int(ceil) + 1, step))
    return ceil, ticks


def _panel_sizes(available_sizes: list[int], wanted_sizes: list[int]) -> list[int]:
    available = set(available_sizes)
    return [size for size in wanted_sizes if size in available]


def _throughput_draw_order(impls: list[str]) -> list[str]:
    order = [
        "rzmq-iouring",
        "libzmq",
        "libzmq-mt",
        "omq-libzmq",
        "omq-tokio-mt",
        "omq-tokio",
        "rzmq",
        "zmq.rs",
    ]
    return [name for name in order if name in impls]


def _fmt_msg_tick(v: float) -> str:
    if v >= 1e6:
        n = v / 1e6
        return f"{n:.1f}M" if n < 10 and n != int(n) else f"{n:.0f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}k"
    return f"{v:.0f}"


def _fmt_gbs_tick(v: float) -> str:
    if v >= 10:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:.1f}" if v != int(v) else f"{v:.0f}"
    return f"{v:.2g}"


def draw_split_throughput_cpu_panel(
    L: list[str],
    sizes: list[int],
    xs: list[float],
    tput: dict,
    tput_cpu: dict,
    impls: list[str],
    x_left: float,
    x_right: float,
    y_top: float,
    y_bot: float,
    title: str,
    metric: str,
    fixed_metric_max: float | None = None,
    log_metric: bool = False,
):
    """Two-axis split panel: CPU% on the left, one throughput metric right."""
    import math

    if not sizes:
        return

    metric_idx = 0 if metric == "msgs" else 1
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_cpu = [
        tput_cpu[size][name]
        for size in sizes
        for name in impls
        if name in tput_cpu.get(size, {})
    ]
    cpu_ceil, cpu_ticks = _cpu_ticks(max(all_cpu) * 1.1 if all_cpu else 200)

    all_metric = [
        tput[size][name][metric_idx]
        for size in sizes
        for name in impls
        if name in tput.get(size, {})
    ]

    if log_metric:
        positive = [v for v in all_metric if v > 0]
        metric_min = min(positive) if positive else 0.01
        metric_ref = max(fixed_metric_max or max(positive or [0.01]), 0.01)
        log_lo = math.floor(math.log10(metric_min * 0.8))
        log_hi = math.ceil(math.log10(metric_ref * 1.15))
        if log_hi <= log_lo:
            log_hi = log_lo + 1
    else:
        metric_max = (
            fixed_metric_max
            if fixed_metric_max
            else (max(all_metric) * 1.15 if all_metric else 1.0)
        )
        metric_max = max(metric_max, 1.0 if metric == "msgs" else 0.001)

    def y_cpu(v):
        frac = max(0, min(1, v / cpu_ceil))
        return y_bot - frac * h

    def y_metric(v):
        if log_metric:
            if v <= 0:
                return y_bot
            frac = (math.log10(v) - log_lo) / (log_hi - log_lo)
            frac = max(0, min(1, frac))
            return y_bot - frac * h
        return y_bot - (v / metric_max) * h

    L.append(
        svg_text(mid_x, y_top - 13, title, size=11.5,
                 weight="700", fill="#111827")
    )

    for val in cpu_ticks:
        yy = y_cpu(val)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{val:g}%",
                          anchor="end", baseline="middle", size=8.5))

    if log_metric:
        for decade in range(log_lo, log_hi + 1):
            base = 10 ** decade
            for mult in [1, 2, 5]:
                v = base * mult
                if v < 10 ** log_lo or v > 10 ** log_hi:
                    continue
                yy = y_metric(v)
                if mult == 1:
                    label = f"{_fmt_gbs_tick(v)} GB/s"
                    L.append(svg_line(x_right, yy, x_right + 4, yy,
                                      stroke="#9ca3af"))
                    L.append(svg_text(x_right + 8, yy, label,
                                      anchor="start", baseline="middle",
                                      size=8.5, fill="#6b7280"))
    else:
        step = nice_step(metric_max, 6)
        v = step
        while v <= metric_max:
            yy = y_metric(v)
            label = (
                f"{_fmt_msg_tick(v)}/s"
                if metric == "msgs"
                else f"{_fmt_gbs_tick(v)} GB/s"
            )
            L.append(svg_line(x_right, yy, x_right + 4, yy,
                              stroke="#9ca3af"))
            L.append(svg_text(x_right + 8, yy, label,
                              anchor="start", baseline="middle",
                              size=8.5, fill="#6b7280"))
            v += step

    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    L.append(svg_line(x_left, y_top, x_left, y_bot,
                      stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot,
                      stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot,
                      stroke="#9ca3af", width=1.5))

    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(x_left - 48, mid_y, "CPU %",
                      weight="600", rotate=-90))

    draw_order = _throughput_draw_order(impls)

    for name in draw_order:
        pts = [
            (xs[i], y_cpu(tput_cpu[sizes[i]][name]))
            for i in range(len(sizes))
            if name in tput_cpu.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name],
                                  width=CPU_LINE_WIDTH,
                                  dash=CPU_LINE_DASH, opacity=0.85))

    for name in draw_order:
        pts = [
            (xs[i], y_metric(tput[sizes[i]][name][metric_idx]))
            for i in range(len(sizes))
            if name in tput.get(sizes[i], {})
        ]
        if not pts:
            continue

        if metric == "msgs":
            for i, size in enumerate(sizes):
                val = tput.get(size, {}).get(name)
                if val and len(val) >= 4 and val[2] > 0 and val[3] > 0:
                    x = xs[i]
                    y1 = y_metric(val[2])
                    y2 = y_metric(val[3])
                    L.append(
                        f'  <line x1="{x:.1f}" y1="{y1:.1f}"'
                        f' x2="{x:.1f}" y2="{y2:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0"'
                        f' opacity="0.45"/>'
                    )
                    L.append(
                        f'  <line x1="{x - 3:.1f}" y1="{y1:.1f}"'
                        f' x2="{x + 3:.1f}" y2="{y1:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0"'
                        f' opacity="0.45"/>'
                    )
                    L.append(
                        f'  <line x1="{x - 3:.1f}" y1="{y2:.1f}"'
                        f' x2="{x + 3:.1f}" y2="{y2:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0"'
                        f' opacity="0.45"/>'
                    )
            L.append(svg_polyline(pts, COLORS[name],
                                  width=METRIC_LINE_WIDTH,
                                  dash=MSG_LINE_DASH))
            zero_pts = []
            nonzero_pts = []
            for i in range(len(sizes)):
                if name not in tput.get(sizes[i], {}):
                    continue
                pt = (xs[i], y_metric(tput[sizes[i]][name][metric_idx]))
                if tput[sizes[i]][name][0] == 0:
                    zero_pts.append(pt)
                else:
                    nonzero_pts.append(pt)
            L.extend(svg_dots(nonzero_pts, COLORS[name],
                              radius=MARKER_RADIUS))
            L.extend(svg_x_marks(zero_pts, COLORS[name],
                                 radius=MARKER_RADIUS))
        else:
            L.append(svg_polyline(pts, COLORS[name],
                                  width=METRIC_LINE_WIDTH))
            zero_pts = []
            nonzero_pts = []
            for i in range(len(sizes)):
                if name not in tput.get(sizes[i], {}):
                    continue
                pt = (xs[i], y_metric(tput[sizes[i]][name][metric_idx]))
                if tput[sizes[i]][name][0] == 0:
                    zero_pts.append(pt)
                else:
                    nonzero_pts.append(pt)
            L.extend(svg_dots(nonzero_pts, COLORS[name],
                              radius=MARKER_RADIUS))
            L.extend(svg_x_marks(zero_pts, COLORS[name],
                                 radius=MARKER_RADIUS))

    for i, size in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(size), size=8.5))


def draw_throughput_cpu_panel(
    L: list[str], sizes: list[int], xs: list[float], tput: dict,
    tput_cpu: dict, impls: list[str],
    x_left: float, x_right: float, x_right2: float,
    y_top: float, y_bot: float, title: str,
    fixed_gbs_max: float | None = None,
    fixed_msg_max: float | None = None,
    log_gbs: bool = False,
):
    """Three-axis throughput panel: CPU% (left, dotted), GB/s (inner right,
    solid+dots), msg/s (outer right, dashed)."""
    import math

    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_cpu = [
        tput_cpu[s][name]
        for s in sizes for name in impls if name in tput_cpu.get(s, {})
    ]
    cpu_ceil, cpu_ticks = _cpu_ticks(max(all_cpu) * 1.1 if all_cpu else 200)

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    gbs_max = max(all_gbs) if all_gbs else 10.0
    gbs_min = min(all_gbs) if all_gbs else 0.01
    if log_gbs:
        gbs_min = max(gbs_min, 0.01)
        log_lo = math.floor(math.log10(gbs_min * 0.8))
        log_ref = max(fixed_gbs_max or gbs_max, 0.01)
        log_hi = math.ceil(math.log10(log_ref * 1.15))
    else:
        gbs_max = fixed_gbs_max if fixed_gbs_max else (gbs_max * 1.15)
        gbs_max = max(gbs_max, 0.001)

    all_msgs = [
        tput[s][name][0]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    msg_max = fixed_msg_max if fixed_msg_max else (max(all_msgs) * 1.15 if all_msgs else 16e6)
    msg_max = max(msg_max, 1.0)

    def y_cpu(v):
        frac = max(0, min(1, v / cpu_ceil))
        return y_bot - frac * h

    def y_gbs(v):
        if log_gbs:
            if v <= 0:
                return y_bot
            frac = (math.log10(v) - log_lo) / (log_hi - log_lo)
            return y_bot - frac * h
        return y_bot - (v / gbs_max) * h

    def y_msg(v):
        return y_bot - (v / msg_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # CPU% gridlines (left axis)
    for val in cpu_ticks:
        yy = y_cpu(val)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{val:g}%",
                          anchor="end", baseline="middle"))

    # GB/s gridlines (inner right axis, dashed)
    if log_gbs:
        for decade in range(log_lo, log_hi + 1):
            base = 10 ** decade
            for mult in [1, 2, 5]:
                v = base * mult
                if v < 10 ** log_lo or v > 10 ** log_hi:
                    continue
                yy = y_gbs(v)
                if mult == 1:
                    L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
                    label = f"{v:.0f}" if v >= 1 else f"{v:g}"
                    L.append(svg_text(x_right + 8, yy, f"{label} GB/s",
                                      anchor="start", baseline="middle",
                                      fill="#6b7280"))
                else:
                    L.append(svg_line(x_left, yy, x_right, yy,
                                      dash="2,8", stroke="#e5e7eb"))
    else:
        step_gbs = nice_step(gbs_max, 5)
        v = step_gbs
        while v <= gbs_max:
            yy = y_gbs(v)
            L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
            L.append(svg_text(x_right + 8, yy, f"{v:.0f} GB/s",
                              anchor="start", baseline="middle", fill="#6b7280"))
            v += step_gbs

    # msg/s tick labels (outer right axis)
    step_msg = nice_step(msg_max, 8)
    v = step_msg
    while v <= msg_max:
        yy = y_msg(v)
        millions = v / 1e6
        if millions >= 1 and millions == int(millions):
            label = f"{int(millions)}M"
        elif v >= 1e6:
            label = f"{millions:.1f}M"
        else:
            label = f"{v / 1e3:.0f}k"
        L.append(svg_text(x_right2 + 8, yy, f"{label}/s",
                          anchor="start", baseline="middle", fill="#9ca3af"))
        v += step_msg

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right2, y_top, x_right2, y_bot, stroke="#d1d5db", width=1))

    # axis labels
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "CPU %", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["rzmq-iouring", "libzmq", "libzmq-mt", "omq-libzmq",
                   "omq-tokio-mt", "omq-tokio", "rzmq", "zmq.rs"]
                  if name in impls]

    # dotted CPU% lines
    for name in draw_order:
        pts = [
            (xs[i], y_cpu(tput_cpu[sizes[i]][name]))
            for i in range(len(sizes)) if name in tput_cpu.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=1.6,
                                  dash="2,5", opacity=0.85))

    # dashed msg/s lines (outer right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_msg(tput[sizes[i]][name][0]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            for i, size in enumerate(sizes):
                val = tput.get(size, {}).get(name)
                if val and len(val) >= 4 and val[2] > 0 and val[3] > 0:
                    x = xs[i]
                    y1 = y_msg(val[2])
                    y2 = y_msg(val[3])
                    L.append(
                        f'  <line x1="{x:.1f}" y1="{y1:.1f}" x2="{x:.1f}" y2="{y2:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0" opacity="0.45"/>'
                    )
                    L.append(
                        f'  <line x1="{x - 3:.1f}" y1="{y1:.1f}" x2="{x + 3:.1f}" y2="{y1:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0" opacity="0.45"/>'
                    )
                    L.append(
                        f'  <line x1="{x - 3:.1f}" y1="{y2:.1f}" x2="{x + 3:.1f}" y2="{y2:.1f}"'
                        f' stroke="{COLORS[name]}" stroke-width="1.0" opacity="0.45"/>'
                    )
            L.append(svg_polyline(pts, COLORS[name], width=2.0, dash="5,3"))

    # solid GB/s lines (inner right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_gbs(tput[sizes[i]][name][1]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2.0))
            zero_pts = []
            nonzero_pts = []
            for i in range(len(sizes)):
                if name not in tput.get(sizes[i], {}):
                    continue
                pt = (xs[i], y_gbs(tput[sizes[i]][name][1]))
                if tput[sizes[i]][name][0] == 0:
                    zero_pts.append(pt)
                else:
                    nonzero_pts.append(pt)
            L.extend(svg_dots(nonzero_pts, COLORS[name], radius=2.2))
            L.extend(svg_x_marks(zero_pts, COLORS[name], radius=3))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def draw_latency_cpu_panel(
    L: list[str], sizes: list[int], xs: list[float], lat: dict,
    lat_cpu: dict, impls: list[str],
    x_left: float, x_right: float,
    y_top: float, y_bot: float, title: str,
    fixed_lat_max: float | None = None,
):
    """Two-axis latency panel: p50 latency (left, solid+dots),
    CPU% (right, dotted)."""
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_vals = [
        lat[s][name]
        for s in sizes for name in impls if name in lat.get(s, {})
    ]
    lat_max = fixed_lat_max if fixed_lat_max else (max(all_vals) * 1.2 if all_vals else 150.0)

    all_cpu = [
        lat_cpu[s][name]
        for s in sizes for name in impls if name in lat_cpu.get(s, {})
    ]
    cpu_ceil, cpu_ticks = _cpu_ticks(max(all_cpu) * 1.1 if all_cpu else 200)

    def y_lat(v):
        return y_bot - (v / lat_max) * h

    def y_cpu(v):
        frac = max(0, min(1, v / cpu_ceil))
        return y_bot - frac * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # latency gridlines (left axis)
    step = nice_step(lat_max, 10)
    v = step
    while v <= lat_max:
        yy = y_lat(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{v:.0f}", anchor="end", baseline="middle"))
        v += step

    # CPU% gridlines (right axis, dashed)
    for val in cpu_ticks:
        yy = y_cpu(val)
        L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
        L.append(svg_text(x_right + 8, yy, f"{val:g}%",
                          anchor="start", baseline="middle", fill="#6b7280"))

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    # axis label
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "p50 latency (µs)", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["rzmq-iouring", "libzmq", "libzmq-mt", "omq-libzmq",
                   "omq-tokio-mt", "omq-tokio", "rzmq", "zmq.rs"]
                  if name in impls]

    # dotted CPU% lines (right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_cpu(lat_cpu[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat_cpu.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=1.6,
                                  dash="2,5", opacity=0.85))

    # solid latency lines with dots (left axis)
    for name in draw_order:
        pts = [
            (xs[i], y_lat(lat[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def nice_step(max_val: float, target_lines: int) -> float:
    raw = max_val / target_lines
    mag = 10 ** int(f"{raw:.0e}".split("e")[1])
    for s in [1, 2, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


# ── chart generation ──────────────────────────────────────────────

def detect_hardware() -> str | None:
    from chart_hw import detect_hardware as _detect
    return _detect()


def _legend_extra(n_items: int, show_st_mt: bool = False) -> float:
    """Pre-compute vertical space consumed by the legend."""
    row_gap = 20
    n_rows = 2 if n_items > 4 else 1
    extra = (n_rows - 1) * row_gap
    if show_st_mt:
        extra += 31
    return extra


def _draw_impl_legend(L: list[str], impls: list[str], mid_x: float, leg_y: float,
                      label_overrides: dict | None = None,
                      show_st_mt: bool = False) -> float:
    """Draw impl legend in up to two rows. Returns extra vertical space consumed."""
    legend_items = [(k, (label_overrides or {}).get(k, LABELS[k])) for k in impls if k in COLORS]
    item_w = 190
    row_gap = 20

    if len(legend_items) > 4:
        mid = (len(legend_items) + 1) // 2
        rows = [legend_items[:mid], legend_items[mid:]]
    else:
        rows = [legend_items]

    extra = 0
    left_x = mid_x
    for row_idx, row in enumerate(rows):
        ry = leg_y + row_idx * row_gap
        total_w = len(row) * item_w
        start_x = mid_x - total_w / 2
        if row_idx == 0:
            left_x = start_x
        for i, (key, label) in enumerate(row):
            lx = start_x + i * item_w
            c = COLORS[key]
            L.append(
                f'  <line x1="{lx:.0f}" y1="{ry}" x2="{lx + 14:.0f}" y2="{ry}"'
                f' stroke="{c}" stroke-width="2.5"/>'
            )
            L.append(f'  <circle cx="{lx + 7:.0f}" cy="{ry}" r="2.5" fill="{c}"/>')
            L.append(
                f'  <text x="{lx + 20:.0f}" y="{ry + 4}" fill="#374151"'
                f' font-size="11" font-weight="500">{label}</text>'
            )
    extra = (len(rows) - 1) * row_gap

    if show_st_mt:
        st_y = leg_y + extra + 18
        L.append(
            f'  <text x="{left_x:.0f}" y="{st_y}"'
            f' fill="#9ca3af" font-size="9">'
            f'(nT) = configurable IO threads</text>'
        )
        L.append(
            f'  <text x="{left_x:.0f}" y="{st_y + 13}"'
            f' fill="#9ca3af" font-size="9">'
            f'[nT] = fixed IO threads</text>'
        )
        extra += 31
    return extra


def generate_chart(data: dict, impls: list[str], transport_label: str,
                   log_gbs: bool = False,
                   fixed_msg_max: float | None = None,
                   fixed_gbs_max: float | None = None,
                   msg_break: tuple[float, float] | None = None,
                   hw_label: str | None = None,
                   label_overrides: dict | None = None) -> str:
    sizes = data["sizes"]
    tput = data["tput"]
    n = len(sizes)
    if n < 2:
        print(f"WARNING: only {n} data points for {transport_label}", file=sys.stderr)
        if n == 0:
            return ""

    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 480 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    t1_y_top = 35 + hw_offset
    t1_y_bot = 385 + hw_offset

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    draw_throughput_panel(
        L, sizes, xs, tput, impls, x_left, x_right, t1_y_top, t1_y_bot,
        f"PUSH/PULL throughput: {transport_label} (higher is better)",
        log_gbs=log_gbs,
        fixed_msg_max=fixed_msg_max,
        fixed_gbs_max=fixed_gbs_max,
        msg_break=msg_break,
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{t1_y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    leg_y = t1_y_bot + 60
    _draw_impl_legend(L, impls, mid_x, leg_y, label_overrides=label_overrides)

    # line-type legend (dashed = msg/s, solid = GB/s)
    lt_y = leg_y + 22
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="6,4"/>'
    )
    L.append(
        f'  <text x="{lt_start + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (left axis)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2"/>'
    )
    gbs_label = "throughput / GB/s (right axis, log)" if log_gbs \
        else "throughput / GB/s (right axis)"
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">{gbs_label}</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_latency_chart(data: dict, impls: list[str], transport_label: str,
                           fixed_lat_max: float | None = None,
                           hw_label: str | None = None,
                           label_overrides: dict | None = None) -> str:
    sizes = data["sizes"]
    lat = data["lat"]
    n = len(sizes)
    if n < 2:
        return ""

    has_latency = any(s in lat and any(name in lat[s] for name in impls) for s in sizes)
    if not has_latency:
        return ""

    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 280 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    y_top = 35 + hw_offset
    y_bot = y_top + 150

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    draw_latency_panel(
        L, sizes, xs, lat, impls, x_left, x_right, y_top, y_bot,
        f"REQ/REP latency: {transport_label} (p50 µs, lower is better)",
        fixed_lat_max=fixed_lat_max,
    )

    leg_y = y_bot + 50
    _draw_impl_legend(L, impls, mid_x, leg_y, label_overrides=label_overrides)

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_chart_cpu(data: dict, impls: list[str], transport_label: str,
                       fixed_gbs_max: float | None = None,
                       fixed_msg_max: float | None = None,
                       log_gbs: bool = False,
                       hw_label: str | None = None,
                       label_overrides: dict | None = None,
                       show_st_mt: bool = False) -> str:
    """Throughput chart split into msg/s and GB/s panels, both with CPU%."""
    sizes = data["sizes"]
    tput = data["tput"]
    tput_cpu = data.get("tput_cpu", {})
    has_data = {name for s in sizes for name in tput.get(s, {})}
    impls = [i for i in impls if i in has_data]
    small_sizes = _panel_sizes(sizes, SMALL_MESSAGE_SIZES)
    large_sizes = _panel_sizes(sizes, LARGE_MESSAGE_SIZES)
    if not small_sizes and not large_sizes:
        print(f"WARNING: no throughput data for {transport_label}",
              file=sys.stderr)
        return ""

    hw_offset = 14 if hw_label else 0
    panel_h = 280
    x_pad_left = 78
    panel_gap_x = 98
    x_pad_right = 78
    legend_h = 86
    svg_w = 980
    total_w = svg_w - x_pad_left - x_pad_right - panel_gap_x
    p1_w = total_w * 0.4
    p2_w = total_w - p1_w
    mid_x = svg_w / 2
    leg_extra = _legend_extra(len(impls), show_st_mt)
    header_y = 17
    row_top = hw_offset + header_y + 42
    row_bot = row_top + panel_h
    svg_h = row_bot + legend_h + leg_extra

    def make_xs(panel_sizes, x_left, x_right):
        return [
            x_left + i * (x_right - x_left) / max(len(panel_sizes) - 1, 1)
            for i in range(len(panel_sizes))
        ]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    L.append(svg_text(mid_x, header_y,
                      f"PUSH/PULL throughput: {transport_label}",
                      size=14, weight="700", fill="#111827"))
    if hw_label:
        L.append(svg_text(mid_x, header_y + 14, hw_label,
                          size=9, fill="#9ca3af"))

    p1_xl = x_pad_left
    p1_xr = p1_xl + p1_w
    draw_split_throughput_cpu_panel(
        L, small_sizes, make_xs(small_sizes, p1_xl, p1_xr),
        tput, tput_cpu, impls, p1_xl, p1_xr, row_top, row_bot,
        "small messages: msg/s", "msgs",
        fixed_metric_max=fixed_msg_max,
    )

    p2_xl = p1_xr + panel_gap_x
    p2_xr = p2_xl + p2_w
    draw_split_throughput_cpu_panel(
        L, large_sizes, make_xs(large_sizes, p2_xl, p2_xr),
        tput, tput_cpu, impls, p2_xl, p2_xr, row_top, row_bot,
        "medium/large messages: GB/s", "gbs",
        fixed_metric_max=fixed_gbs_max,
        log_metric=log_gbs,
    )

    leg_y = row_bot + 38
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    # line-type legend
    lt_y = leg_y + 22 + extra
    lt_total = 570
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{CPU_LINE_WIDTH}"'
        f' stroke-dasharray="{CPU_LINE_DASH}" opacity="0.85"/>'
    )
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (left)</text>'
    )

    lt_mid = lt_start + 145
    L.append(
        f'  <line x1="{lt_mid:.0f}" y1="{lt_y}" x2="{lt_mid + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{METRIC_LINE_WIDTH}"'
        f' stroke-dasharray="{MSG_LINE_DASH}"/>'
    )
    L.append(
        f'  <text x="{lt_mid + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (small panel)</text>'
    )

    lt_right = lt_mid + 190
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{METRIC_LINE_WIDTH}"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">GB/s (large panel{", log" if log_gbs else ""})</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_latency_chart_cpu(data: dict, impls: list[str], transport_label: str,
                               fixed_lat_max: float | None = None,
                               hw_label: str | None = None,
                               label_overrides: dict | None = None,
                               show_st_mt: bool = False) -> str:
    """Latency chart with two axes: p50 latency (left), CPU% (right, dotted)."""
    sizes = data["sizes"]
    lat = data["lat"]
    lat_cpu = data.get("lat_cpu", {})
    has_data = {name for s in sizes for name in lat.get(s, {})}
    impls = [i for i in impls if i in has_data]
    n = len(sizes)
    if n < 2:
        return ""

    has_latency = any(s in lat and any(name in lat[s] for name in impls) for s in sizes)
    if not has_latency:
        return ""

    hw_offset = 14 if hw_label else 0
    leg_extra = _legend_extra(len(impls), show_st_mt)
    svg_w = 850
    svg_h = 320 + hw_offset + leg_extra
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    y_top = 35 + hw_offset
    y_bot = y_top + 180

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    draw_latency_cpu_panel(
        L, sizes, xs, lat, lat_cpu, impls, x_left, x_right, y_top, y_bot,
        f"REQ/REP latency: {transport_label} (p50 µs)",
        fixed_lat_max=fixed_lat_max,
    )

    leg_y = y_bot + 40
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    # line-type legend
    lt_y = leg_y + 22 + extra
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2.5"/>'
    )
    L.append(f'  <circle cx="{lt_start + 7:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">p50 latency (left)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="1.6" stroke-dasharray="2,5" opacity="0.85"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (right)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def load_pubsub_data(transport: str, impls: list[str], peers: int) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows
              if r.get("transport") == transport
              and r.get("kind") == "pub_sub"
              and r.get("peers") == peers]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    seen: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        cpu_time = r.get("pub_cpu_time", r.get("cpu_time", 0))
        elapsed = r.get("elapsed", 0)
        if elapsed <= 0 or (cpu_time <= 0 and not r.get("zero_transport")):
            continue
        seq = r.get("_seq", 0)
        size = r.get("msg_size")
        key = (impl_name, size)
        if key not in seen or seq >= seen[key]:
            seen[key] = seq
            msgs_s = r.get("msgs_s", 0)
            mbps = r.get("mbps", 0)
            # mbps is already aggregate (per-sub × peers) from run_comparisons.
            gbs = mbps / 1000.0
            tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)
            if elapsed > 0 and cpu_time > 0:
                tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s in COMPARISON_CHART_SIZES)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu}


def generate_pubsub_chart(
    panels: list[tuple[int, dict]],
    impls: list[str], transport_label: str,
    log_gbs: bool = False,
    fixed_msg_max: float | None = None,
    fixed_gbs_max: float | None = None,
    scale_overrides: dict[int, tuple[float, float | None, bool | None]] | None = None,
    hw_label: str | None = None,
    title_fn: "Callable[[int, str], str] | None" = None,
) -> str:
    panels = [(p, d) for p, d in panels if d["sizes"]]
    if not panels:
        return ""
    sizes = panels[0][1]["sizes"]
    n = len(sizes)
    if n < 2:
        return ""

    panel_h = 240
    gap = 70
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = hw_offset + 35 + len(panels) * (panel_h + gap) + 20 + _legend_extra(len(impls))
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{hw_offset + 32}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    for idx, (peers, data) in enumerate(panels):
        p_sizes = data["sizes"]
        p_xs = [x_left + i * plot_w / max(len(p_sizes) - 1, 1)
                for i in range(len(p_sizes))]
        y_top = hw_offset + 35 + idx * (panel_h + gap)
        y_bot = y_top + panel_h
        if title_fn:
            panel_title = title_fn(peers, transport_label)
        else:
            sub_label = "1 subscriber" if peers == 1 else f"{peers} subscribers"
            panel_title = f"PUB/SUB throughput, {sub_label}: {transport_label}"
        p_msg_max = fixed_msg_max
        p_gbs_max = fixed_gbs_max
        p_log_gbs = log_gbs
        if scale_overrides and peers in scale_overrides:
            ovr = scale_overrides[peers]
            p_msg_max = ovr[0]
            p_gbs_max = ovr[1]
            if len(ovr) > 2 and ovr[2] is not None:
                p_log_gbs = ovr[2]
        draw_throughput_panel(
            L, p_sizes, p_xs, data["tput"], impls,
            x_left, x_right, y_top, y_bot,
            panel_title,
            log_gbs=p_log_gbs,
            fixed_msg_max=p_msg_max,
            fixed_gbs_max=p_gbs_max,
        )

    last_bot = hw_offset + 35 + (len(panels) - 1) * (panel_h + gap) + panel_h
    leg_y = last_bot + 40
    extra = _draw_impl_legend(L, impls, mid_x, leg_y)

    lt_y = leg_y + 22 + extra
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="6,4"/>'
    )
    L.append(
        f'  <text x="{lt_start + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (left axis)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2"/>'
    )
    gbs_label = "throughput / GB/s (right axis, log)" if log_gbs \
        else "throughput / GB/s (right axis)"
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">{gbs_label}</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def load_fanio_data(transport: str, impls: list[str], peers: int,
                     kind: str) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows
              if r.get("transport") == transport
              and r.get("kind") == kind
              and r.get("peers") == peers]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    seen: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        seq = r.get("_seq", 0)
        size = r.get("msg_size")
        key = (impl_name, size)
        if key not in seen or seq >= seen[key]:
            seen[key] = seq
            msgs_s = r.get("msgs_s", 0)
            mbps = r.get("mbps", 0)
            gbs = mbps / 1000.0
            tput.setdefault(size, {})[impl_name] = (
                msgs_s,
                gbs,
                r.get("peer_min", 0),
                r.get("peer_max", 0),
            )
            if kind == "fan_out":
                cpu_time = r.get("push_cpu_time", 0)
            else:
                cpu_time = r.get("pull_cpu_time", r.get("cpu_time", 0))
            elapsed = r.get("elapsed", 0)
            if elapsed > 0 and cpu_time > 0:
                tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s in COMPARISON_CHART_SIZES)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu}


def generate_multi_panel_cpu_chart(
    panels: list[tuple[int, dict]],
    impls: list[str], transport_label: str,
    hw_label: str | None = None,
    title_fn: "Callable[[int, str], str] | None" = None,
    label_overrides: dict | None = None,
    show_st_mt: bool = False,
) -> str:
    panels = [(p, d) for p, d in panels if d["sizes"]]
    if not panels:
        return ""

    has_data = {
        name
        for _peers, data in panels
        for size in data["sizes"]
        for name in data["tput"].get(size, {})
    }
    impls = [i for i in impls if i in has_data]
    if not impls:
        return ""

    panel_h = 235
    row_gap = 82
    x_pad_left = 78
    panel_gap_x = 98
    x_pad_right = 78
    hw_offset = 14 if hw_label else 0
    leg_extra = _legend_extra(len(impls), show_st_mt)
    svg_w = 1020
    total_w = svg_w - x_pad_left - x_pad_right - panel_gap_x
    p1_w = total_w * 0.4
    p2_w = total_w - p1_w
    row_start = hw_offset + 72
    row_step = panel_h + row_gap
    last_bot = row_start + (len(panels) - 1) * row_step + panel_h
    svg_h = last_bot + 112 + leg_extra
    mid_x = svg_w / 2

    def make_xs(panel_sizes, x_left, x_right):
        return [
            x_left + i * (x_right - x_left) / max(len(panel_sizes) - 1, 1)
            for i in range(len(panel_sizes))
        ]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(svg_text(mid_x, 32, hw_label, size=9, fill="#9ca3af"))

    for idx, (peers, data) in enumerate(panels):
        small_sizes = _panel_sizes(data["sizes"], SMALL_MESSAGE_SIZES)
        large_sizes = _panel_sizes(data["sizes"], LARGE_MESSAGE_SIZES)
        y_top = row_start + idx * row_step
        y_bot = y_top + panel_h
        if title_fn:
            row_title = title_fn(peers, transport_label)
        else:
            row_title = f"throughput, {peers} peers: {transport_label}"
        L.append(svg_text(mid_x, y_top - 35, row_title,
                          size=13, weight="700", fill="#111827"))

        p1_xl = x_pad_left
        p1_xr = p1_xl + p1_w
        draw_split_throughput_cpu_panel(
            L, small_sizes, make_xs(small_sizes, p1_xl, p1_xr),
            data["tput"], data.get("tput_cpu", {}), impls,
            p1_xl, p1_xr, y_top, y_bot,
            "small messages: msg/s", "msgs",
        )

        p2_xl = p1_xr + panel_gap_x
        p2_xr = p2_xl + p2_w
        draw_split_throughput_cpu_panel(
            L, large_sizes, make_xs(large_sizes, p2_xl, p2_xr),
            data["tput"], data.get("tput_cpu", {}), impls,
            p2_xl, p2_xr, y_top, y_bot,
            "medium/large messages: GB/s", "gbs",
        )

    leg_y = last_bot + 34
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    lt_y = leg_y + 22 + extra
    lt_total = 570
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{CPU_LINE_WIDTH}"'
        f' stroke-dasharray="{CPU_LINE_DASH}" opacity="0.85"/>'
    )
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (left)</text>'
    )

    lt_mid = lt_start + 145
    L.append(
        f'  <line x1="{lt_mid:.0f}" y1="{lt_y}" x2="{lt_mid + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{METRIC_LINE_WIDTH}"'
        f' stroke-dasharray="{MSG_LINE_DASH}"/>'
    )
    L.append(
        f'  <text x="{lt_mid + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (small panel)</text>'
    )

    lt_right = lt_mid + 190
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="{METRIC_LINE_WIDTH}"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">GB/s (large panel)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    FIXED_GBS_MAX = 6.0
    FIXED_LAT_MAX = 150.0
    FIXED_INPROC_LAT_MAX = 40.0
    hw = detect_hardware()

    # ── Cross-impl charts ──────────────────────────────────────
    label_overrides = {
        "omq-tokio": "omq-tokio (1T)",
        "zmq.rs": "zmq.rs v0.6.0 [6T]",
        "rzmq": "rzmq v0.5.24 [6T]",
        "rzmq-iouring": "rzmq v0.5.24 (io_uring) [6T]",
    }
    label_for = {
        "tcp": "TCP loopback, 2-process",
        "ipc": "IPC, 2-process",
        "inproc": "inproc",
    }

    tcp_impls = ["libzmq", "libzmq-mt", "omq-tokio", "omq-tokio-mt", "omq-libzmq", "zmq.rs", "rzmq", "rzmq-iouring"]
    ipc_impls = ["libzmq", "libzmq-mt", "omq-tokio", "omq-tokio-mt", "zmq.rs", "rzmq", "rzmq-iouring"]
    inproc_impls = ["libzmq", "omq-tokio", "omq-tokio-mt", "rzmq", "rzmq-iouring"]

    # (transport, impls, log).
    cross_charts = [
        ("tcp", tcp_impls, False),
        ("ipc", ipc_impls, False),
        ("inproc", inproc_impls, True),
    ]

    for transport, impls, log in cross_charts:
        data = load_data(transport, impls)
        if not data["sizes"]:
            continue
        label = label_for[transport]

        svg = generate_chart_cpu(data, impls, label,
                                 fixed_gbs_max=None if log else FIXED_GBS_MAX,
                                 log_gbs=log,
                                 hw_label=hw,
                                 label_overrides=label_overrides,
                                 show_st_mt=True)
        if svg:
            out = REPO / "doc" / "charts" / "pushpull" / f"{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

        lat_max = FIXED_INPROC_LAT_MAX if transport == "inproc" else FIXED_LAT_MAX
        svg = generate_latency_chart_cpu(data, impls, label,
                                         fixed_lat_max=lat_max, hw_label=hw,
                                         label_overrides=label_overrides,
                                         show_st_mt=True)
        if svg:
            out = REPO / "doc" / "charts" / "reqrep" / f"{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

    # ── PUB/SUB charts ──────────────────────────────────────────
    pubsub_peer_counts = [1, 8, 32]

    def pubsub_title(peers, tl):
        sub_label = "1 subscriber" if peers == 1 else f"{peers} subscribers"
        return f"PUB/SUB throughput, {sub_label}: {tl}"

    panels = [
        (p, load_pubsub_data("tcp", tcp_impls, p))
        for p in pubsub_peer_counts
    ]
    if any(d["sizes"] for _, d in panels):
        svg = generate_multi_panel_cpu_chart(
            panels, tcp_impls, "TCP loopback",
            hw_label=hw, title_fn=pubsub_title,
            label_overrides=label_overrides, show_st_mt=True,
        )
        if svg:
            out = REPO / "doc" / "charts" / "pubsub" / "tcp.svg"
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

    # ── Fan-out / fan-in charts (TCP only) ──────────────────────
    fanio_peers = [2, 4, 8]

    def fanout_title(peers, tl):
        return f"PUSH fan-out (1 PUSH → {peers} PULL): {tl}"

    def fanin_title(peers, tl):
        return f"PUSH fan-in ({peers} PUSH → 1 PULL): {tl}"

    for kind, tfn, dir_name in [
        ("fan_out", fanout_title, "fanout"),
        ("fan_in", fanin_title, "fanin"),
    ]:
        panels = [
            (p, load_fanio_data("tcp", tcp_impls, p, kind))
            for p in fanio_peers
        ]
        if not any(d["sizes"] for _, d in panels):
            continue
        svg = generate_multi_panel_cpu_chart(
            panels, tcp_impls, "TCP loopback",
            hw_label=hw,
            title_fn=tfn,
            label_overrides=label_overrides,
            show_st_mt=True,
        )
        if svg:
            out = REPO / "doc" / "charts" / "pushpull" / dir_name / "tcp.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)


    # ── Main chart ─────────────────────────────────────────────
    from gen_main_chart import (MAIN_DRAW_ORDER, MAIN_IMPLS, MAIN_TITLE,
                                generate_main_chart,
                                load_data as load_main_data)
    tput, _lat, msgs = load_main_data()
    svg = generate_main_chart(tput, msgs, MAIN_IMPLS, MAIN_DRAW_ORDER,
                              MAIN_TITLE, hw)
    if svg:
        out = REPO / "doc" / "charts" / "main_tcp.svg"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
