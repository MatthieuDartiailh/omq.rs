#!/usr/bin/env python3
"""Generate comparison SVG charts from benchmarks/comparisons.jsonl.

Produces:
  doc/charts/comparison.svg        — TCP: throughput + latency (4 impls)
  doc/charts/comparison_inproc.svg — inproc: throughput + latency (3 impls, no zmq.rs)
"""

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
JSONL_PATH = REPO / "benchmarks" / "comparisons.jsonl"

COLORS = {
    "libzmq": "#eab308",
    "omq-compio": "#dc2626",
    "omq-compio-st": "#f87171",
    "omq-tokio": "#f97316",
    "zmq.rs": "#2563eb",
}

LABELS = {
    "libzmq": "libzmq v4.3.5",
    "omq-compio": "omq-compio",
    "omq-compio-st": "omq-compio (st)",
    "omq-tokio": "omq-tokio",
    "zmq.rs": "zmq.rs v0.6.0",
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
    title: str,
):
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_msgs = [
        tput[s][name][0]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    msg_max = max(all_msgs) * 1.15 if all_msgs else 16e6

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    tput_max = max(all_gbs) * 1.15 if all_gbs else 10.0

    def y_msg(v):
        return y_bot - (v / msg_max) * h

    def y_tput(v):
        return y_bot - (v / tput_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # msg/s gridlines (left axis)
    step_msg = nice_step(msg_max, 5)
    v = step_msg
    while v <= msg_max:
        yy = y_msg(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        label = f"{v / 1e6:.0f}M" if v >= 1e6 else f"{v / 1e3:.0f}k"
        L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
        v += step_msg

    # GB/s gridlines (right axis, dashed)
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

    # axis labels
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "msg/s", weight="600", rotate=-90))
    L.append(svg_text(812, mid_y, "throughput", weight="600",
                      fill="#6b7280", rotate=90))

    # dashed msg/s lines
    draw_order = [name for name in ["zmq.rs", "libzmq", "omq-tokio", "omq-compio"]
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
    title: str,
):
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_vals = [
        lat[s][name]
        for s in sizes for name in impls if name in lat.get(s, {})
    ]
    lat_max = max(all_vals) * 1.2 if all_vals else 150.0

    def y_lat(v):
        return y_bot - (v / lat_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # gridlines
    step = nice_step(lat_max, 6)
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

    draw_order = [name for name in ["libzmq", "omq-tokio", "zmq.rs", "omq-compio"]
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
    for s in [1, 2, 2.5, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


# ── chart generation ──────────────────────────────────────────────

def generate_chart(data: dict, impls: list[str], transport_label: str) -> str:
    sizes = data["sizes"]
    tput = data["tput"]
    lat = data["lat"]
    n = len(sizes)
    if n < 2:
        print(f"WARNING: only {n} data points for {transport_label}", file=sys.stderr)
        if n == 0:
            return ""

    has_latency = any(s in lat and any(name in lat[s] for name in impls) for s in sizes)

    svg_w, svg_h = 850, 640 if has_latency else 340
    x_left, x_right = 90, 760
    plot_w = x_right - x_left

    t1_y_top, t1_y_bot = 35, 270

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    draw_throughput_panel(
        L, sizes, xs, tput, impls, x_left, x_right, t1_y_top, t1_y_bot,
        f"PUSH/PULL throughput — 2-process, {transport_label} (higher is better)",
    )

    if has_latency:
        t2_y_top, t2_y_bot = 350, 540
        draw_latency_panel(
            L, sizes, xs, lat, impls, x_left, x_right, t2_y_top, t2_y_bot,
            f"REQ/REP latency — 2-process, {transport_label} (p50 µs, lower is better)",
        )
        leg_y = t2_y_bot + 60
    else:
        leg_y = t1_y_bot + 60

    # legend
    mid_x = (x_left + x_right) / 2
    legend_items = [(k, LABELS[k]) for k in impls if k in COLORS]
    item_w = 140
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

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    # TCP chart (4 impls)
    tcp_impls = ["libzmq", "omq-compio", "omq-tokio", "zmq.rs"]
    tcp_data = load_data("tcp", tcp_impls)

    if tcp_data["sizes"]:
        svg = generate_chart(tcp_data, tcp_impls, "TCP loopback")
        out = REPO / "doc" / "charts" / "comparison.svg"
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)
    else:
        print("No TCP data found", file=sys.stderr)

    # Inproc chart (4 impls: libzmq, compio mt+st, tokio; no zmq.rs)
    inproc_impls = ["libzmq", "omq-compio", "omq-compio-st", "omq-tokio"]
    inproc_data = load_data("inproc", inproc_impls)

    if inproc_data["sizes"]:
        svg = generate_chart(inproc_data, inproc_impls, "inproc")
        out = REPO / "doc" / "charts" / "comparison_inproc.svg"
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)
    else:
        print("No inproc data found", file=sys.stderr)


if __name__ == "__main__":
    main()
