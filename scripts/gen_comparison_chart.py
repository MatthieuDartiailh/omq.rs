#!/usr/bin/env python3
"""Generate doc/charts/throughput.svg from COMPARISONS.md data."""

import re
import sys
from pathlib import Path


def parse_msgs_s(s: str) -> float:
    s = s.strip().replace(",", "")
    if s.endswith("M"):
        return float(s[:-1]) * 1e6
    if s.endswith("k"):
        return float(s[:-1]) * 1e3
    return float(s)


def parse_throughput(s: str) -> float:
    """Parse '68 MB/s', '1.8 GB/s' etc. into GB/s."""
    s = s.strip()
    m = re.match(r"([\d.]+)\s*(MB/s|GB/s)", s)
    if not m:
        raise ValueError(f"Cannot parse throughput: {s!r}")
    val = float(m.group(1))
    if m.group(2) == "MB/s":
        val /= 1024
    return val


def parse_size_bytes(s: str) -> int:
    s = s.strip()
    m = re.match(r"([\d.]+)\s*(B|KiB|MiB)", s)
    if not m:
        raise ValueError(f"Cannot parse size: {s!r}")
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


def parse_table(text: str, marker: str) -> list[dict]:
    begin = f"<!-- BEGIN {marker} -->"
    end = f"<!-- END {marker} -->"
    section = text.split(begin)[1].split(end)[0]
    rows = []
    for line in section.strip().splitlines():
        line = line.strip()
        if not line.startswith("|") or "---" in line:
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if cells[0] in ("Size", ""):
            continue
        rows.append({
            "size": parse_size_bytes(cells[0]),
            "ref_msgs": parse_msgs_s(cells[1]),
            "ref_tput": parse_throughput(cells[2]),
            "omq_msgs": parse_msgs_s(cells[3]),
            "omq_tput": parse_throughput(cells[4]),
        })
    return rows


def load_data(comparisons_md: Path) -> dict:
    text = comparisons_md.read_text()

    libzmq_compio = parse_table(text, "libzmq_comparison_tcp_compio")
    libzmq_tokio = parse_table(text, "libzmq_comparison_tcp_tokio")
    zmqrs_compio = parse_table(text, "zmqrs_comparison_tcp_compio")
    zmqrs_tokio = parse_table(text, "zmqrs_comparison_tcp_tokio")

    max_size = 512 * 1024
    sizes = sorted({r["size"] for r in libzmq_compio if r["size"] <= max_size})

    def by_size(rows):
        return {r["size"]: r for r in rows}

    lc, lt = by_size(libzmq_compio), by_size(libzmq_tokio)
    zc, zt = by_size(zmqrs_compio), by_size(zmqrs_tokio)

    series = {}
    for s in sizes:
        series.setdefault("libzmq", []).append((lc[s]["ref_msgs"], lc[s]["ref_tput"]))
        series.setdefault("compio", []).append((lc[s]["omq_msgs"], lc[s]["omq_tput"]))
        series.setdefault("tokio", []).append((lt[s]["omq_msgs"], lt[s]["omq_tput"]))
        series.setdefault("zmqrs", []).append((zc[s]["ref_msgs"], zc[s]["ref_tput"]))

    return {"sizes": sizes, "series": series}


def generate_svg(data: dict) -> str:
    sizes = data["sizes"]
    series = data["series"]
    n = len(sizes)

    x_left, x_right = 90, 760
    y_top, y_bot = 45, 350
    svg_h = 440
    plot_w = x_right - x_left
    plot_h = y_bot - y_top

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    import math

    # Log scale for msg/s: 10k .. 10M (3 decades)
    msg_log_min = 4.0   # log10(10k)
    msg_log_max = 7.0   # log10(10M)

    tput_max = 10.0  # GB/s, linear

    def y_msg(v):
        if v <= 0:
            return y_bot
        log_v = math.log10(v)
        frac = (log_v - msg_log_min) / (msg_log_max - msg_log_min)
        return y_bot - frac * plot_h

    def y_tput(v):
        return y_bot - (v / tput_max) * plot_h

    colors = {
        "libzmq": "#eab308",
        "compio": "#dc2626",
        "tokio": "#f97316",
        "zmqrs": "#2563eb",
    }
    labels = {
        "libzmq": "libzmq",
        "compio": "omq-compio",
        "tokio": "omq-tokio",
        "zmqrs": "zmq.rs",
    }
    draw_order = ["zmqrs", "libzmq", "compio", "tokio"]
    legend_order = ["libzmq", "compio", "tokio", "zmqrs"]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="850" height="{svg_h}" fill="white"/>')

    # Left-axis gridlines: msg/s log scale
    # Major gridlines at decade boundaries (solid, labeled)
    for exp, label in [(3, "1k"), (4, "10k"), (5, "100k"), (6, "1M"), (7, "10M")]:
        yy = y_bot - (exp - msg_log_min) / (msg_log_max - msg_log_min) * plot_h
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">{label}</text>'
        )
    # Minor gridlines at 2x, 3x, 5x within each decade (lighter, labeled)
    minor_labels = {
        (3, 2): "2k", (3, 3): "3k", (3, 5): "5k",
        (4, 2): "20k", (4, 3): "30k", (4, 5): "50k",
        (5, 2): "200k", (5, 3): "300k", (5, 5): "500k",
        (6, 2): "2M", (6, 3): "3M", (6, 5): "5M",
    }
    for base_exp in range(int(msg_log_min), int(msg_log_max)):
        for mult in [2, 3, 5]:
            log_v = base_exp + math.log10(mult)
            if log_v <= msg_log_min or log_v >= msg_log_max:
                continue
            yy = y_bot - (log_v - msg_log_min) / (msg_log_max - msg_log_min) * plot_h
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}"'
                f' y2="{yy:.1f}" stroke="#f0f0f0" stroke-width="0.5"/>'
            )
            label = minor_labels.get((base_exp, mult), "")
            if label:
                L.append(
                    f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
                    f' dominant-baseline="middle" fill="#9ca3af"'
                    f' font-size="8">{label}</text>'
                )

    # Right-axis gridlines: throughput linear (dashed)
    for v in [2, 4, 6, 8, 10]:
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
        f' transform="rotate(-90,40,{mid_y:.1f})">msg/s (log)</text>'
    )
    L.append(
        f'  <text x="812" y="{mid_y:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#6b7280" font-size="11" font-weight="600"'
        f' transform="rotate(90,812,{mid_y:.1f})">throughput</text>'
    )
    L.append(
        f'  <text x="{mid_x:.1f}" y="22" text-anchor="middle" fill="#111827"'
        f' font-size="14" font-weight="700">PUSH/PULL throughput: TCP loopback</text>'
    )

    # Dashed throughput lines
    for name in draw_order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_tput(series[name][i][1]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    # Solid msg/s lines with dots
    for name in draw_order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_msg(series[name][i][0]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i in range(n):
            yy = y_msg(series[name][i][0])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # Legend
    leg_y1 = y_bot + 38
    leg_y2 = leg_y1 + 12
    legend_xs = [167, 327, 487, 647]
    for i, name in enumerate(legend_order):
        lx = legend_xs[i]
        c = colors[name]
        L.append(
            f'  <line x1="{lx}" y1="{leg_y1}" x2="{lx + 14}" y2="{leg_y1}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <line x1="{lx}" y1="{leg_y2}" x2="{lx + 14}" y2="{leg_y2}"'
            f' stroke="{c}" stroke-width="2" stroke-dasharray="4,3"/>'
        )
        L.append(
            f'  <text x="{lx + 18}" y="{leg_y1 + 4}" fill="#374151" font-size="10"'
            f' font-weight="500">{labels[name]}</text>'
        )

    footer_y = y_bot + 68
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"solid = msg/s (left, log) · dashed = throughput (right, linear)</text>"
    )
    L.append("</svg>")

    return "\n".join(L) + "\n"


def main():
    repo = Path(__file__).resolve().parent.parent
    comparisons_md = repo / "COMPARISONS.md"

    if not comparisons_md.exists():
        print(f"ERROR: {comparisons_md} not found", file=sys.stderr)
        sys.exit(1)

    data = load_data(comparisons_md)
    svg = generate_svg(data)

    output = repo / "doc" / "charts" / "throughput.svg"
    output.write_text(svg)
    print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
