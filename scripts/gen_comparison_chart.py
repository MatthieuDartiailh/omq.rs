#!/usr/bin/env python3
"""Generate comparison SVG charts from benchmarks/comparisons.jsonl.

Produces:
  doc/charts/comparison_tcp.svg    — TCP: throughput + latency (4 impls)
  doc/charts/comparison_ipc.svg    — IPC: throughput + latency (4 impls)
  doc/charts/comparison_inproc.svg — inproc: throughput + latency (4 impls)
"""

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"

COLORS = {
    "libzmq": "#eab308",
    "omq-compio": "#dc2626",
    "omq-compio-st": "#8b5cf6",
    "omq-tokio": "#f97316",
    "zmq.rs": "#2563eb",
    "rzmq": "#10b981",
    "omq-libzmq": "#06b6d4",
}

LABELS = {
    "libzmq": "libzmq v4.3.5",
    "omq-compio": "omq-compio (MT)",
    "omq-compio-st": "omq-compio (ST)",
    "omq-tokio": "omq-tokio",
    "zmq.rs": "zmq.rs v0.6.0",
    "rzmq": "rzmq v0.5.15",
    "omq-libzmq": "omq-libzmq",
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
    for line in JSONL_PATH.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def load_data(transport: str, impls: list[str]) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows if r.get("transport") == transport]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    lat: dict[int, dict[str, float]] = {}

    seen_tput: dict[tuple, str] = {}
    seen_lat: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        kind = r.get("kind")

        if kind == "throughput":
            key = (impl_name, size)
            if key not in seen_tput or run_id >= seen_tput[key]:
                seen_tput[key] = run_id
                msgs_s = r.get("msgs_s", 0)
                mbps = r.get("mbps", 0)
                gbs = mbps / 1000.0
                tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)

        elif kind == "latency":
            key = (impl_name, size)
            if key not in seen_lat or run_id >= seen_lat[key]:
                seen_lat[key] = run_id
                lat.setdefault(size, {})[impl_name] = r.get("p50_us", 0)

    sizes = sorted(s for s in tput if s <= 32768)
    return {"sizes": sizes, "tput": tput, "lat": lat}


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
                 dash=None) -> str:
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    d = f' stroke-dasharray="{dash}"' if dash else ""
    cap = ' stroke-linecap="round" stroke-linejoin="round"' if not dash else ""
    return (
        f'  <polyline points="{pts}" fill="none" stroke="{color}"'
        f' stroke-width="{width}"{cap}{d}/>'
    )


def svg_dots(points: list[tuple[float, float]], color: str) -> list[str]:
    return [
        f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="3"'
        f' fill="{color}" stroke="white" stroke-width="1"/>'
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

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    gbs_max = max(all_gbs) if all_gbs else 10.0
    gbs_min = min(all_gbs) if all_gbs else 0.01
    if log_gbs:
        gbs_min = max(gbs_min, 0.01)
        log_lo = math.floor(math.log10(gbs_min * 0.8))
        log_hi = math.ceil(math.log10((fixed_gbs_max or gbs_max) * 1.15))
    else:
        tput_max = fixed_gbs_max if fixed_gbs_max else gbs_max * 1.15

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
    draw_order = [name for name in ["rzmq", "zmq.rs", "libzmq", "omq-libzmq", "omq-tokio", "omq-compio-st", "omq-compio"]
                  if name in impls]
    for name in draw_order:
        pts = [
            (xs[i], y_msg(tput[sizes[i]][name][0]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2, dash="6,4"))

    # solid throughput lines with dots
    for name in draw_order:
        pts = [
            (xs[i], y_tput(tput[sizes[i]][name][1]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

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

    draw_order = [name for name in ["libzmq", "omq-libzmq", "omq-tokio", "rzmq", "zmq.rs", "omq-compio-st", "omq-compio"]
                  if name in impls]
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
    try:
        cpu = None
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                break
        cores = os.cpu_count()
        if cpu and cores:
            label = f"{cpu}, {cores} cores"
            extras = []
            # Detect governor.
            try:
                gov = open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read().strip()
                if gov == "performance":
                    extras.append("performance governor")
            except OSError:
                pass
            # Detect turbo boost state (Intel pstate or generic cpufreq).
            for path, off_val in [
                ("/sys/devices/system/cpu/intel_pstate/no_turbo", "1"),
                ("/sys/devices/system/cpu/cpufreq/boost", "0"),
            ]:
                try:
                    if open(path).read().strip() == off_val:
                        extras.append("turbo off")
                    break
                except OSError:
                    continue
            # Override via env for machines where sysfs isn't available.
            if not extras:
                hw_extras = os.environ.get("OMQ_HW_EXTRAS")
                if hw_extras:
                    extras.extend(hw_extras.split(","))
            if extras:
                label += ", " + ", ".join(extras)
            return label
    except OSError:
        pass
    return None


def _draw_impl_legend(L: list[str], impls: list[str], mid_x: float, leg_y: float):
    legend_items = [(k, LABELS[k]) for k in impls if k in COLORS]
    item_w = 125
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = COLORS[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
        )


def generate_chart(data: dict, impls: list[str], transport_label: str,
                   log_gbs: bool = False,
                   fixed_msg_max: float | None = None,
                   fixed_gbs_max: float | None = None,
                   msg_break: tuple[float, float] | None = None,
                   hw_label: str | None = None) -> str:
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
    _draw_impl_legend(L, impls, mid_x, leg_y)

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
    L.append(f'  <circle cx="{lt_right + 10:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
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
                           hw_label: str | None = None) -> str:
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
    _draw_impl_legend(L, impls, mid_x, leg_y)

    L.append("</svg>")
    return "\n".join(L) + "\n"


def load_pubsub_data(transport: str, impls: list[str], peers: int) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows
              if r.get("transport") == transport
              and r.get("kind") == "pub_sub"
              and r.get("peers") == peers]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    seen: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        key = (impl_name, size)
        if key not in seen or run_id >= seen[key]:
            seen[key] = run_id
            msgs_s = r.get("msgs_s", 0)
            mbps = r.get("mbps", 0)
            # mbps is already aggregate (per-sub × peers) from run_comparisons.
            gbs = mbps / 1000.0
            tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)

    sizes = sorted(s for s in tput if s <= 32768)
    return {"sizes": sizes, "tput": tput}


def generate_pubsub_chart(
    panels: list[tuple[int, dict]],
    impls: list[str], transport_label: str,
    log_gbs: bool = False,
    fixed_msg_max: float | None = None,
    fixed_gbs_max: float | None = None,
    scale_overrides: dict[int, tuple[float, float | None, bool | None]] | None = None,
    hw_label: str | None = None,
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
    svg_h = hw_offset + 35 + len(panels) * (panel_h + gap) + 20
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
        sub_label = "1 subscriber" if peers == 1 else f"{peers} subscribers"
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
            f"PUB/SUB throughput, {sub_label}: {transport_label}",
            log_gbs=p_log_gbs,
            fixed_msg_max=p_msg_max,
            fixed_gbs_max=p_gbs_max,
        )

    last_bot = hw_offset + 35 + (len(panels) - 1) * (panel_h + gap) + panel_h
    leg_y = last_bot + 40

    legend_items = [(k, LABELS[k]) for k in impls if k in COLORS]
    item_w = 125
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = COLORS[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

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
    L.append(f'  <circle cx="{lt_right + 10:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    gbs_label = "throughput / GB/s (right axis, log)" if log_gbs \
        else "throughput / GB/s (right axis)"
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">{gbs_label}</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    FIXED_MSG_MAX = 20e6
    FIXED_GBS_MAX = 6.0
    FIXED_LAT_MAX = 150.0
    FIXED_INPROC_LAT_MAX = 40.0
    hw = detect_hardware()

    tcp_impls = ["libzmq", "omq-compio", "omq-tokio", "zmq.rs", "rzmq", "omq-libzmq"]
    ipc_impls = ["libzmq", "omq-compio", "omq-tokio", "zmq.rs", "rzmq", "omq-libzmq"]
    inproc_impls = ["libzmq", "omq-compio", "omq-compio-st", "omq-tokio", "rzmq", "omq-libzmq"]

    for transport, impls, label, log_gbs in [
        ("tcp", tcp_impls, "TCP loopback, 2-process", False),
        ("ipc", ipc_impls, "IPC, 2-process", False),
        ("inproc", inproc_impls, "inproc", True),
    ]:
        data = load_data(transport, impls)
        if not data["sizes"]:
            print(f"No {transport} data found", file=sys.stderr)
            continue

        msg_max = 10e6 if transport == "inproc" else FIXED_MSG_MAX
        msg_break = (1e6, 0.25) if not log_gbs else None
        svg = generate_chart(data, impls, label, log_gbs=log_gbs,
                             fixed_msg_max=msg_max,
                             fixed_gbs_max=None if log_gbs else FIXED_GBS_MAX,
                             msg_break=msg_break,
                             hw_label=hw)
        out = REPO / "doc" / "charts" / "pushpull" / f"comparison_{transport}.svg"
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)

        lat_max = FIXED_INPROC_LAT_MAX if transport == "inproc" else FIXED_LAT_MAX
        svg = generate_latency_chart(data, impls, label,
                                     fixed_lat_max=lat_max, hw_label=hw)
        if svg:
            out = REPO / "doc" / "charts" / "reqrep" / f"comparison_{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

    # PUB/SUB charts
    PUBSUB_MSG_MAX = 10e6
    PUBSUB_GBS_MAX = 8.0
    pubsub_impls = ["libzmq", "omq-compio", "omq-tokio", "zmq.rs", "rzmq", "omq-libzmq"]
    pubsub_peer_counts = [1, 8, 64]

    for transport, label, log in [
        ("tcp", "TCP loopback", False),
        ("ipc", "IPC", False),
    ]:
        panels = [
            (p, load_pubsub_data(transport, pubsub_impls, p))
            for p in pubsub_peer_counts
        ]
        if not any(d["sizes"] for _, d in panels):
            continue
        svg = generate_pubsub_chart(
            panels, pubsub_impls, label,
            log_gbs=log,
            fixed_msg_max=PUBSUB_MSG_MAX,
            fixed_gbs_max=None if log else PUBSUB_GBS_MAX,
            scale_overrides={
                1: (PUBSUB_MSG_MAX, 6.0),
                8: (2e6, PUBSUB_GBS_MAX),
                64: (300e3, 12.0),
            },
            hw_label=hw,
        )
        if svg:
            out = REPO / "doc" / "charts" / "pubsub" / f"comparison_{transport}.svg"
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
