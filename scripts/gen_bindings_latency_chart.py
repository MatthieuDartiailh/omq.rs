#!/usr/bin/env python3
"""Generate doc/charts/latency_bindings.svg from pyomq README latency table.

Grouped bar chart: x = message sizes, bars = pyomq vs pyzmq, y = p50 latency (log).
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
    w, h = 850, 440
    left, right, top, bottom = 80, 30, 45, 70
    plot_w = w - left - right
    plot_h = h - top - bottom

    all_vals = [v for sz in sizes for v in data[sz].values() if v > 0]
    if not all_vals:
        return "<svg xmlns='http://www.w3.org/2000/svg' width='850' height='440'></svg>"

    y_min = 10 ** math.floor(math.log10(min(all_vals)))
    y_max = 10 ** math.ceil(math.log10(max(all_vals)))
    log_range = math.log10(y_max) - math.log10(y_min)

    def y_pos(val):
        if val <= 0:
            return top + plot_h
        return top + plot_h - (math.log10(val) - math.log10(y_min)) / log_range * plot_h

    n_groups = len(sizes)
    n_bars = len(SERIES)
    group_w = plot_w / n_groups
    bar_w = group_w * 0.5 / n_bars
    gap = group_w * 0.5

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
        f'font-family="system-ui,-apple-system,sans-serif" font-size="11">',
        f'<rect width="{w}" height="{h}" fill="#fff"/>',
        f'<text x="{w//2}" y="22" text-anchor="middle" font-size="14" font-weight="bold">'
        f'REQ/REP latency: pyomq vs pyzmq, TCP loopback (p50)</text>',
    ]

    decade = y_min
    while decade <= y_max:
        y = y_pos(decade)
        lines.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                     f'stroke="#e5e7eb" stroke-width="0.5"/>')
        label = f"{decade:.0f} µs" if decade < 1000 else f"{decade/1000:.0f} ms"
        lines.append(f'<text x="{left-8}" y="{y:.1f}" text-anchor="end" '
                     f'dominant-baseline="middle" font-size="10" fill="#6b7280">{label}</text>')
        decade *= 10

    for gi, size in enumerate(sizes):
        gx = left + gi * group_w + gap / 2
        for bi, name in enumerate(SERIES):
            val = data[size].get(name)
            if val is None or val <= 0:
                continue
            bx = gx + bi * bar_w
            by = y_pos(val)
            bh = top + plot_h - by
            color = COLORS[name]
            lines.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" '
                         f'height="{bh:.1f}" fill="{color}" opacity="0.85"/>')
            label = f"{val:.0f}" if val >= 10 else f"{val:.1f}"
            lines.append(f'<text x="{bx + bar_w/2:.1f}" y="{by - 3:.1f}" '
                         f'text-anchor="middle" font-size="9" fill="#374151">{label}</text>')

        cx = gx + n_bars * bar_w / 2
        lines.append(f'<text x="{cx:.1f}" y="{top + plot_h + 16}" text-anchor="middle" '
                     f'font-size="10" fill="#374151">{fmt_size(size)}</text>')

    lines.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" '
                 f'stroke="#374151" stroke-width="1"/>')
    lines.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" '
                 f'stroke="#374151" stroke-width="1"/>')
    lines.append(f'<text x="14" y="{top + plot_h//2}" text-anchor="middle" '
                 f'font-size="11" fill="#374151" '
                 f'transform="rotate(-90 14 {top + plot_h//2})">p50 latency (µs, log)</text>')

    lx = left + 10
    ly = top + 10
    for i, name in enumerate(SERIES):
        lines.append(f'<rect x="{lx}" y="{ly + i*18}" width="12" height="12" fill="{COLORS[name]}"/>')
        lines.append(f'<text x="{lx+16}" y="{ly + i*18 + 10}" font-size="10" fill="#374151">{name}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


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
