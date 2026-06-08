#!/usr/bin/env python3
"""Generate doc/charts/mechanism/{compio,tokio}.svg from bench JSONL data."""

import json
import math
import os
import sys
from pathlib import Path


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def _nice_ceil(v):
    if v <= 0:
        return 1
    exp = math.floor(math.log10(v))
    base = 10 ** exp
    for m in [1, 2, 5, 10]:
        candidate = m * base
        if candidate >= v:
            return candidate
    return 10 * base


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def load_data(jsonl: Path) -> dict:
    rows = []
    for line in jsonl.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("pattern") == "mechanism":
            rows.append(r)

    if not rows:
        return {"sizes": [], "series": {}}

    latest: dict[tuple[str, int], dict] = {}
    for r in rows:
        key = (r["transport"], r["msg_size"])
        latest[key] = r

    mechanisms = ["NULL", "PLAIN", "CURVE", "BLAKE3ZMQ"]
    all_sizes = sorted({k[1] for k in latest})
    sizes = [s for s in all_sizes if all((m, s) in latest for m in mechanisms)]

    size_filter = os.environ.get("OMQ_CHART_SIZES")
    if size_filter:
        allowed = {int(x) for x in size_filter.split(",") if x.strip()}
        sizes = [s for s in sizes if s in allowed]

    series: dict[str, list[tuple[float, float]]] = {m: [] for m in mechanisms}
    for s in sizes:
        for m in mechanisms:
            r = latest[(m, s)]
            mbps = r["mbps"]
            msgs = r["msgs_s"]
            series[m].append((msgs, mbps / 1024))

    return {"sizes": sizes, "series": series}


def generate_svg(data: dict, backend: str) -> str:
    sizes = data["sizes"]
    series = data["series"]
    n = len(sizes)

    x_left, x_right = 90, 760
    y_top, y_bot = 45, 350
    svg_h = 440
    plot_w = x_right - x_left
    plot_h = y_bot - y_top

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    all_msgs = [pt[0] for pts in series.values() for pt in pts if pt[0] > 0]
    msg_max = _nice_ceil(max(all_msgs)) if all_msgs else 10e6
    tput_max = 10.0    # GB/s

    def y_msg(v):
        if v <= 0:
            return y_bot
        return y_bot - (v / msg_max) * plot_h

    def y_tput(v):
        return y_bot - (v / tput_max) * plot_h

    colors = {"NULL": "#9ca3af", "PLAIN": "#374151", "CURVE": "#dc2626", "BLAKE3ZMQ": "#2563eb"}
    labels = {"NULL": "NULL", "PLAIN": "PLAIN", "CURVE": "CURVE", "BLAKE3ZMQ": "BLAKE3ZMQ"}
    order = ["NULL", "PLAIN", "CURVE", "BLAKE3ZMQ"]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="850" height="{svg_h}" fill="white"/>')

    # Left-axis: msg/s linear scale
    n_l_ticks = 5
    for i in range(n_l_ticks + 1):
        val = i * msg_max / n_l_ticks
        yy = y_msg(val)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">'
            f'{_fmt_y_rate(val)}</text>'
        )

    # Right-axis: throughput (dashed)
    for v in [2, 4, 6, 8]:
        yy = y_tput(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>'
        )
        L.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#6b7280" font-size="10">'
            f'{v} GB/s</text>'
        )

    # Vertical gridlines
    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{y_top}" x2="{x:.1f}" y2="{y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    # Axes
    L.append(
        f'  <line x1="{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_right}" y1="{y_top}" x2="{x_right}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    # X-axis labels
    for i, s in enumerate(sizes):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{y_bot + 16}" text-anchor="middle"'
            f' fill="#374151" font-size="9.5">{fmt_size(s)}</text>'
        )

    # Axis titles
    mid_y = (y_top + y_bot) / 2
    mid_x = (x_left + x_right) / 2
    L.append(
        f'  <text x="40" y="{mid_y:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="11" font-weight="600"'
        f' transform="rotate(-90,40,{mid_y:.1f})">msg/s</text>'
    )
    L.append(
        f'  <text x="812" y="{mid_y:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#6b7280" font-size="11" font-weight="600"'
        f' transform="rotate(90,812,{mid_y:.1f})">throughput</text>'
    )
    L.append(
        f'  <text x="{mid_x:.1f}" y="22" text-anchor="middle" fill="#111827"'
        f' font-size="14" font-weight="700">'
        f'PUSH/PULL throughput: mechanism overhead (omq-{backend}, TCP)</text>'
    )

    # Dashed msg/s lines
    for name in order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_msg(series[name][i][0]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    # Solid throughput lines with dots
    for name in order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_tput(series[name][i][1]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i in range(n):
            yy = y_tput(series[name][i][1])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # Legend
    leg_y1 = y_bot + 38
    leg_y2 = leg_y1 + 12
    legend_xs = [107, 277, 477, 647]
    for i, name in enumerate(order):
        lx = legend_xs[i]
        c = colors[name]
        L.append(
            f'  <line x1="{lx}" y1="{leg_y1}" x2="{lx + 14}" y2="{leg_y1}"'
            f' stroke="{c}" stroke-width="2" stroke-dasharray="4,3"/>'
        )
        L.append(
            f'  <line x1="{lx}" y1="{leg_y2}" x2="{lx + 14}" y2="{leg_y2}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <text x="{lx + 18}" y="{leg_y1 + 4}" fill="#374151" font-size="10"'
            f' font-weight="500">{labels[name]}</text>'
        )

    footer_y = y_bot + 68
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f'dashed = msg/s (left) · solid = throughput (right)</text>'
    )
    L.append("</svg>")

    return "\n".join(L) + "\n"


def main():
    cache_dir = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
    repo = Path(__file__).resolve().parent.parent
    out_dir = repo / "doc" / "charts" / "mechanism"
    out_dir.mkdir(parents=True, exist_ok=True)

    backends = sys.argv[1:] if len(sys.argv) > 1 else ["compio", "tokio"]

    for backend in backends:
        jsonl = cache_dir / f"results_{backend}.jsonl"
        if not jsonl.exists():
            print(f"SKIP: {jsonl} not found", file=sys.stderr)
            continue

        data = load_data(jsonl)
        if not data["sizes"]:
            print(f"SKIP: no mechanism data in {jsonl.name}. Run: "
                  f"cargo bench -p omq-{backend} --bench mechanism "
                  f"--features 'plain curve blake3zmq'", file=sys.stderr)
            continue

        svg = generate_svg(data, backend)
        output = out_dir / f"{backend}.svg"
        output.write_text(svg)
        print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
