#!/usr/bin/env python3
"""Generate doc/charts/comparison_latency_chart.svg from COMPARISONS.md latency tables.

Grouped bar chart: x = message sizes, bars = implementations, y = p50 latency (log).
"""

import math
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COMPARISONS = REPO / "COMPARISONS.md"
OUTPUT = REPO / "doc" / "charts" / "comparison_latency_chart.svg"

COLORS = {
    "libzmq": "#eab308",
    "omq-compio": "#dc2626",
    "omq-tokio": "#f97316",
    "zmq.rs": "#2563eb",
}

SERIES_ORDER = ["libzmq", "omq-compio", "omq-tokio", "zmq.rs"]


def parse_us(s: str) -> float | None:
    s = s.strip().replace("—", "").replace("—", "")
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
    unit = m.group(2)
    if unit == "KiB":
        n *= 1024
    elif unit == "MiB":
        n *= 1024 * 1024
    return int(n)


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def extract_marker(text: str, marker: str) -> str:
    pattern = rf"<!-- BEGIN {re.escape(marker)} -->\n(.*?)<!-- END {re.escape(marker)} -->"
    m = re.search(pattern, text, re.DOTALL)
    return m.group(1) if m else ""


def parse_latency_table(block: str, ref_name: str):
    """Parse a latency table block. Returns {size_bytes: {impl_name: p50_us}}."""
    data: dict[int, dict[str, float]] = {}
    for line in block.strip().splitlines():
        if not line.startswith("|") or line.startswith("|--") or "Size" in line:
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) < 4:
            continue
        size = parse_size_bytes(cells[0])
        if size == 0:
            continue
        data.setdefault(size, {})
        ref_p50 = parse_us(cells[1])
        if ref_p50:
            data[size][ref_name] = ref_p50
        compio_p50 = parse_us(cells[3])
        if compio_p50:
            data[size]["omq-compio"] = compio_p50
        tokio_p50 = parse_us(cells[6]) if len(cells) > 6 else None
        if tokio_p50:
            data[size]["omq-tokio"] = tokio_p50
    return data


def merge_data(d1, d2):
    merged = {}
    for d in (d1, d2):
        for size, impls in d.items():
            merged.setdefault(size, {}).update(impls)
    return merged


def generate_svg(data: dict[int, dict[str, float]]) -> str:
    if not data:
        return "<svg xmlns='http://www.w3.org/2000/svg' width='850' height='440'></svg>"

    sizes = sorted(data.keys())
    series = [s for s in SERIES_ORDER if any(s in data.get(sz, {}) for sz in sizes)]

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
        log_v = math.log10(val) - math.log10(y_min)
        return top + plot_h - (log_v / log_range) * plot_h

    n_groups = len(sizes)
    n_bars = len(series)
    group_w = plot_w / n_groups
    bar_w = group_w * 0.7 / max(n_bars, 1)
    gap = group_w * 0.3

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
        f'font-family="system-ui,-apple-system,sans-serif" font-size="11">',
        f'<rect width="{w}" height="{h}" fill="#fff"/>',
        f'<text x="{w//2}" y="22" text-anchor="middle" font-size="14" font-weight="bold">'
        f'REQ/REP latency: TCP loopback (p50)</text>',
    ]

    # Y axis gridlines
    decade = y_min
    while decade <= y_max:
        y = y_pos(decade)
        lines.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" '
                     f'stroke="#e5e7eb" stroke-width="0.5"/>')
        label = f"{decade:.0f} µs" if decade < 1000 else f"{decade/1000:.0f} ms"
        lines.append(f'<text x="{left-8}" y="{y:.1f}" text-anchor="end" '
                     f'dominant-baseline="middle" font-size="10" fill="#6b7280">{label}</text>')
        decade *= 10

    # Bars
    for gi, size in enumerate(sizes):
        gx = left + gi * group_w + gap / 2
        for bi, name in enumerate(series):
            val = data[size].get(name)
            if val is None or val <= 0:
                continue
            bx = gx + bi * bar_w
            by = y_pos(val)
            bh = top + plot_h - by
            color = COLORS.get(name, "#888")
            lines.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" '
                         f'height="{bh:.1f}" fill="{color}" opacity="0.85"/>')
            # Value label above bar
            label = f"{val:.0f}" if val >= 10 else f"{val:.1f}"
            lines.append(f'<text x="{bx + bar_w/2:.1f}" y="{by - 3:.1f}" '
                         f'text-anchor="middle" font-size="8" fill="#374151">{label}</text>')

        # X axis label
        cx = gx + n_bars * bar_w / 2
        lines.append(f'<text x="{cx:.1f}" y="{top + plot_h + 16}" text-anchor="middle" '
                     f'font-size="10" fill="#374151">{fmt_size(size)}</text>')

    # Axes
    lines.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" '
                 f'stroke="#374151" stroke-width="1"/>')
    lines.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" '
                 f'stroke="#374151" stroke-width="1"/>')

    # Y axis title
    lines.append(f'<text x="14" y="{top + plot_h//2}" text-anchor="middle" '
                 f'font-size="11" fill="#374151" '
                 f'transform="rotate(-90 14 {top + plot_h//2})">p50 latency (µs, log)</text>')

    # Legend
    lx = left + 10
    ly = top + 10
    for i, name in enumerate(series):
        color = COLORS.get(name, "#888")
        lines.append(f'<rect x="{lx}" y="{ly + i*18}" width="12" height="12" fill="{color}"/>')
        lines.append(f'<text x="{lx+16}" y="{ly + i*18 + 10}" font-size="10" '
                     f'fill="#374151">{name}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def main():
    text = COMPARISONS.read_text()

    libzmq_tcp = extract_marker(text, "libzmq_latency_tcp")
    zmqrs_tcp = extract_marker(text, "zmqrs_latency_tcp")

    d1 = parse_latency_table(libzmq_tcp, "libzmq")
    d2 = parse_latency_table(zmqrs_tcp, "zmq.rs")
    data = merge_data(d1, d2)

    if not data:
        print("No latency data found in COMPARISONS.md. Run the latency benchmarks first.")
        sys.exit(0)

    svg = generate_svg(data)
    OUTPUT.write_text(svg)
    print(f"Generated {OUTPUT}")


if __name__ == "__main__":
    main()
