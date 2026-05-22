#!/usr/bin/env python3
"""Generate doc/charts/latency_bindings.svg from pyomq README latency table.

Line chart: x = message sizes, y = p50 latency (log).
"""

import math
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "bindings" / "pyomq" / "README.md"
OUTPUT = REPO / "doc" / "charts" / "latency_bindings.svg"

COLORS = {"pyomq": "#dc2626", "pyzmq": "#2563eb"}
SERIES = ["pyomq", "pyzmq"]


def parse_us(s: str) -> float | None:
    s = s.strip()
    m = re.match(r"([\d.]+)\s*(µs|ms)", s)
    if not m:
        return None
    val = float(m.group(1))
    if m.group(2) == "ms":
        val *= 1000
    return val


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


def parse_latency_table(block: str) -> dict[int, dict[str, float]]:
    data: dict[int, dict[str, float]] = {}
    for line in block.strip().splitlines():
        if not line.startswith("|") or line.startswith("|--") or "Size" in line:
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) < 3:
            continue
        size = parse_size_bytes(cells[0])
        if size == 0:
            continue
        omq_p50 = parse_us(cells[1])
        pz_p50 = parse_us(cells[2])
        data[size] = {}
        if omq_p50:
            data[size]["pyomq"] = omq_p50
        if pz_p50:
            data[size]["pyzmq"] = pz_p50
    return data


def generate_svg(data: dict[int, dict[str, float]]) -> str:
    if not data:
        return "<svg xmlns='http://www.w3.org/2000/svg' width='850' height='440'></svg>"

    sizes = sorted(data.keys())
    n = len(sizes)

    x_left, x_right = 90, 760
    y_top, y_bot = 45, 350
    svg_h = 440
    plot_w = x_right - x_left
    plot_h = y_bot - y_top

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)] if n > 1 else [
        (x_left + x_right) / 2]

    all_vals = [v for sz in sizes for v in data[sz].values() if v > 0]
    if not all_vals:
        return "<svg xmlns='http://www.w3.org/2000/svg' width='850' height='440'></svg>"

    y_max_val = max(all_vals) * 1.1

    def y_pos(val):
        if val <= 0:
            return y_bot
        return y_bot - (val / y_max_val) * plot_h

    def fmt_us(v):
        if v >= 1000:
            return f"{v / 1000:.0f} ms" if v % 1000 == 0 else f"{v / 1000:.1f} ms"
        return f"{v:.0f} µs" if v == int(v) else f"{v:.1f} µs"

    def nice_ticks(vmax, target_count=8):
        raw = vmax / target_count
        mag = 10 ** math.floor(math.log10(raw))
        for step in [1, 2, 5, 10, 20, 50, 100]:
            s = step * mag
            if vmax / s <= target_count + 2:
                return s
        return mag * 10

    tick_step = nice_ticks(y_max_val)

    L = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="850" height="{svg_h}" fill="white"/>',
    ]

    # Y-axis gridlines (linear, evenly spaced)
    v = 0
    while v <= y_max_val:
        yy = y_pos(v)
        L.append(f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
                 f' stroke="#e5e7eb" stroke-width="1"/>')
        if v > 0:
            L.append(f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
                     f' dominant-baseline="middle" fill="#374151" font-size="10">'
                     f'{fmt_us(v)}</text>')
        v += tick_step

    # Vertical gridlines
    for x in xs:
        L.append(f'  <line x1="{x:.1f}" y1="{y_top}" x2="{x:.1f}" y2="{y_bot}"'
                 f' stroke="#e5e7eb" stroke-width="1"/>')

    # Axes
    L.append(f'  <line x1="{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}"'
             f' stroke="#9ca3af" stroke-width="1.5"/>')
    L.append(f'  <line x1="{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}"'
             f' stroke="#9ca3af" stroke-width="1.5"/>')

    # X-axis labels
    for i, s in enumerate(sizes):
        L.append(f'  <text x="{xs[i]:.1f}" y="{y_bot + 16}" text-anchor="middle"'
                 f' fill="#374151" font-size="9.5">{fmt_size(s)}</text>')

    # Axis titles
    mid_y = (y_top + y_bot) / 2
    mid_x = (x_left + x_right) / 2
    L.append(f'  <text x="40" y="{mid_y:.1f}" text-anchor="middle"'
             f' dominant-baseline="middle" fill="#374151" font-size="11" font-weight="600"'
             f' transform="rotate(-90,40,{mid_y:.1f})">p50 latency (log)</text>')
    L.append(f'  <text x="{mid_x:.1f}" y="22" text-anchor="middle" fill="#111827"'
             f' font-size="14" font-weight="700">'
             f'REQ/REP latency: pyomq vs pyzmq, TCP loopback (p50)</text>')

    # Lines with dots (pyzmq drawn first = behind)
    for name in reversed(SERIES):
        color = COLORS[name]
        points = []
        for i, sz in enumerate(sizes):
            val = data[sz].get(name)
            if val and val > 0:
                points.append((xs[i], y_pos(val)))
        if not points:
            continue
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        L.append(f'  <polyline points="{pts}" fill="none" stroke="{color}"'
                 f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>')
        for x, y in points:
            L.append(f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="3"'
                     f' fill="{color}" stroke="white" stroke-width="1"/>')

    # Legend
    leg_y = y_bot + 38
    leg_spacing = plot_w / len(SERIES)
    for i, name in enumerate(SERIES):
        lx = x_left + int(i * leg_spacing + leg_spacing * 0.3)
        L.append(f'  <line x1="{lx}" y1="{leg_y}" x2="{lx + 14}" y2="{leg_y}"'
                 f' stroke="{COLORS[name]}" stroke-width="2.5"/>')
        L.append(f'  <text x="{lx + 18}" y="{leg_y + 4}" fill="#374151" font-size="10"'
                 f' font-weight="500">{name}</text>')

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    text = README.read_text()
    block = extract_marker(text, "LATENCY_PERF")
    data = parse_latency_table(block)

    if not data:
        print("No latency data in pyomq README. Run update_perf.py first.")
        sys.exit(0)

    svg = generate_svg(data)
    OUTPUT.write_text(svg)
    print(f"Generated {OUTPUT}")


if __name__ == "__main__":
    main()
