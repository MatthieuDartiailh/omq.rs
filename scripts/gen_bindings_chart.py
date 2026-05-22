#!/usr/bin/env python3
"""Generate doc/comparison_chart_bindings.svg from pyomq README throughput table.

Dual-axis line chart: x = message sizes, solid = msg/s (log, left),
dashed = throughput in GB/s (linear, right).
"""

import math
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "bindings" / "pyomq" / "README.md"
OUTPUT = REPO / "doc" / "comparison_chart_bindings.svg"

COLORS = {"pyomq": "#dc2626", "pyzmq": "#eab308"}
SERIES = ["pyomq", "pyzmq"]


def parse_rate(s: str) -> float:
    s = s.strip().rstrip("/s").strip()
    m = re.match(r"([\d.]+)\s*(M|k)", s)
    if not m:
        return 0
    val = float(m.group(1))
    return val * 1e6 if m.group(2) == "M" else val * 1e3


def parse_size_bytes(s: str) -> int:
    s = s.strip()
    m = re.match(r"([\d.]+)\s*(B|KiB|MiB)", s)
    if not m:
        return 0
    n = float(m.group(1))
    if m.group(2) == "KiB":
        n *= 1024
    elif m.group(2) == "MiB":
        n *= 1024 * 1024
    return int(n)


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def extract_marker(text: str, marker: str) -> str:
    pattern = rf"<!-- {re.escape(marker)}:START -->\n(.*?)\n<!-- {re.escape(marker)}:END -->"
    m = re.search(pattern, text, re.DOTALL)
    return m.group(1) if m else ""


def parse_throughput_table(block: str) -> dict[int, dict[str, float]]:
    data: dict[int, dict[str, float]] = {}
    for line in block.strip().splitlines():
        if not line.startswith("|") or line.startswith("|--") or "Size" in line:
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) < 7:
            continue
        size = parse_size_bytes(cells[0])
        if size == 0:
            continue
        tcp_omq = parse_rate(cells[4])
        tcp_pz = parse_rate(cells[5])
        data[size] = {}
        if tcp_omq > 0:
            data[size]["pyomq"] = tcp_omq
        if tcp_pz > 0:
            data[size]["pyzmq"] = tcp_pz
    return data


def generate_svg(data: dict[int, dict[str, float]]) -> str:
    sizes = sorted(data.keys())
    w, h = 850, 440
    left, right, top, bottom = 90, 90, 45, 90
    plot_w = w - left - right
    plot_h = 305

    rate_max = 2e6
    tp_ceil = 5

    def y_rate(val):
        if val <= 0:
            return top + plot_h
        return top + plot_h - val / rate_max * plot_h

    def y_tp(gbps):
        return top + plot_h - gbps / tp_ceil * plot_h

    n = len(sizes)
    xs = [left + i * plot_w / (n - 1) if n > 1 else left + plot_w / 2 for i in range(n)]

    L = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
        f'font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="{w}" height="{h}" fill="white"/>',
    ]

    for tick in [0, 200_000, 400_000, 600_000, 800_000, 1_000_000,
                  1_200_000, 1_400_000, 1_600_000, 1_800_000, 2_000_000]:
        y = y_rate(tick)
        major = tick % 500_000 == 0
        if tick >= 1e6:
            label = f"{tick/1e6:.1f}M".replace(".0M", "M")
        elif tick >= 1e3:
            label = f"{tick/1e3:.0f}k"
        else:
            label = "0"
        if major:
            L.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                     f'stroke="#e5e7eb" stroke-width="1"/>')
            L.append(f'  <text x="{left-8}" y="{y:.1f}" text-anchor="end" '
                     f'dominant-baseline="middle" fill="#374151" font-size="10">{label}</text>')
        else:
            L.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                     f'stroke="#f0f0f0" stroke-width="0.5"/>')
            L.append(f'  <text x="{left-8}" y="{y:.1f}" text-anchor="end" '
                     f'dominant-baseline="middle" fill="#9ca3af" font-size="8">{label}</text>')

    for g in range(1, tp_ceil + 1):
        y = y_tp(g)
        L.append(f'  <line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                 f'stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>')
        L.append(f'  <text x="{left+plot_w+8}" y="{y:.1f}" text-anchor="start" '
                 f'dominant-baseline="middle" fill="#6b7280" font-size="10">{g} GB/s</text>')

    for x in xs:
        L.append(f'  <line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" '
                 f'stroke="#e5e7eb" stroke-width="1"/>')

    L.append(f'  <line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" '
             f'stroke="#9ca3af" stroke-width="1.5"/>')
    L.append(f'  <line x1="{left+plot_w}" y1="{top}" x2="{left+plot_w}" y2="{top+plot_h}" '
             f'stroke="#9ca3af" stroke-width="1.5"/>')
    L.append(f'  <line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" '
             f'stroke="#9ca3af" stroke-width="1.5"/>')

    for i, sz in enumerate(sizes):
        L.append(f'  <text x="{xs[i]:.1f}" y="{top+plot_h+16}" text-anchor="middle" '
                 f'fill="#374151" font-size="9.5">{fmt_size(sz)}</text>')

    mid_y = top + plot_h // 2
    L.append(f'  <text x="40" y="{mid_y}" text-anchor="middle" dominant-baseline="middle" '
             f'fill="#374151" font-size="11" font-weight="600" '
             f'transform="rotate(-90,40,{mid_y})">msg/s</text>')
    rx = w - 38
    L.append(f'  <text x="{rx}" y="{mid_y}" text-anchor="middle" dominant-baseline="middle" '
             f'fill="#6b7280" font-size="11" font-weight="600" '
             f'transform="rotate(90,{rx},{mid_y})">throughput</text>')

    cx = left + plot_w / 2
    L.append(f'  <text x="{cx:.1f}" y="22" text-anchor="middle" fill="#111827" '
             f'font-size="14" font-weight="700">'
             f'PUSH/PULL throughput: TCP loopback (Python bindings)</text>')

    for name in SERIES:
        color = COLORS[name]
        tp_pts = []
        for i, sz in enumerate(sizes):
            rate = data[sz].get(name, 0)
            if rate > 0:
                tp_pts.append(f"{xs[i]:.1f},{y_tp(rate * sz / 1024**3):.1f}")
        if tp_pts:
            L.append(f'  <polyline points="{" ".join(tp_pts)}" fill="none" '
                     f'stroke="{color}" stroke-width="2" stroke-dasharray="6,4"/>')

    for name in SERIES:
        color = COLORS[name]
        pts = []
        for i, sz in enumerate(sizes):
            rate = data[sz].get(name, 0)
            if rate > 0:
                pts.append((xs[i], y_rate(rate)))
        if pts:
            L.append(f'  <polyline points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in pts)}" '
                     f'fill="none" stroke="{color}" stroke-width="2.5" '
                     f'stroke-linecap="round" stroke-linejoin="round"/>')
            for x, y in pts:
                L.append(f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{color}" '
                         f'stroke="white" stroke-width="1"/>')

    lx1, lx2 = cx - 118, cx + 62
    ly = top + plot_h + 38
    for i, (lx, name) in enumerate([(lx1, "pyomq"), (lx2, "pyzmq")]):
        color = COLORS[name]
        L.append(f'  <line x1="{lx}" y1="{ly}" x2="{lx+14}" y2="{ly}" '
                 f'stroke="{color}" stroke-width="2.5"/>')
        L.append(f'  <line x1="{lx}" y1="{ly+12}" x2="{lx+14}" y2="{ly+12}" '
                 f'stroke="{color}" stroke-width="2" stroke-dasharray="4,3"/>')
        L.append(f'  <text x="{lx+18}" y="{ly+4}" fill="#374151" font-size="10" '
                 f'font-weight="500">{name}</text>')

    L.append(f'  <text x="{cx:.1f}" y="{ly+30}" text-anchor="middle" fill="#9ca3af" '
             f'font-size="9">solid = msg/s (left) · dashed = throughput (right)</text>')
    L.append("</svg>")
    return "\n".join(L)


def main():
    text = README.read_text()
    block = extract_marker(text, "PERF")
    data = parse_throughput_table(block)

    if not data:
        print("No throughput data in pyomq README. Run update_perf.py first.")
        sys.exit(1)

    svg = generate_svg(data)
    OUTPUT.write_text(svg)
    print(f"Generated {OUTPUT}")


if __name__ == "__main__":
    main()
