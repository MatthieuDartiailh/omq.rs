#!/usr/bin/env python3
"""Generate doc/charts/comparison.svg from COMPARISONS.md data.

Two-panel SVG:
  Top: PUSH/PULL throughput (msg/s, log scale)
  Bottom: REQ/REP latency (p50 µs, linear)

Throughput: libzmq, omq-compio, omq-tokio, zmq.rs.
Latency: libzmq, omq-compio, omq-tokio.
"""

import math
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
    s = s.strip()
    m = re.match(r"([\d.]+)\s*(MB/s|GB/s)", s)
    if not m:
        raise ValueError(f"Cannot parse throughput: {s!r}")
    val = float(m.group(1))
    if m.group(2) == "MB/s":
        val /= 1024
    return val


def parse_latency_us(s: str) -> float:
    s = s.strip()
    m = re.match(r"([\d.]+)\s*µs", s)
    if not m:
        raise ValueError(f"Cannot parse latency: {s!r}")
    return float(m.group(1))


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


def parse_throughput_table(text: str, marker: str) -> list[dict]:
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


def parse_latency_table(text: str, marker: str) -> list[dict]:
    begin = f"<!-- BEGIN {marker} -->"
    end = f"<!-- END {marker} -->"
    parts = text.split(begin)
    if len(parts) < 2:
        return []
    section = parts[1].split(end)[0]
    rows = []
    for line in section.strip().splitlines():
        line = line.strip()
        if not line.startswith("|") or "---" in line:
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if cells[0] in ("Size", ""):
            continue
        try:
            rows.append({
                "size": parse_size_bytes(cells[0]),
                "libzmq_p50": parse_latency_us(cells[1]),
                "compio_p50": parse_latency_us(cells[3]),
                "tokio_p50": parse_latency_us(cells[6]),
            })
        except (ValueError, IndexError):
            continue
    return rows


def load_data(comparisons_md: Path) -> dict:
    text = comparisons_md.read_text()

    libzmq_compio = parse_throughput_table(text, "libzmq_comparison_tcp_compio")
    libzmq_tokio = parse_throughput_table(text, "libzmq_comparison_tcp_tokio")
    zmqrs_compio = parse_throughput_table(text, "zmqrs_comparison_tcp_compio")

    tput = {}
    for r in libzmq_compio:
        s = r["size"]
        tput.setdefault(s, {})
        tput[s]["libzmq"] = (r["ref_msgs"], r["ref_tput"])
        tput[s]["compio"] = (r["omq_msgs"], r["omq_tput"])
    for r in libzmq_tokio:
        s = r["size"]
        tput.setdefault(s, {})
        tput[s]["tokio"] = (r["omq_msgs"], r["omq_tput"])
    for r in zmqrs_compio:
        s = r["size"]
        tput.setdefault(s, {})
        tput[s]["zmqrs"] = (r["ref_msgs"], r["ref_tput"])

    latency_rows = parse_latency_table(text, "libzmq_latency_tcp")
    lat = {}
    for r in latency_rows:
        lat[r["size"]] = {
            "libzmq": r["libzmq_p50"],
            "compio": r["compio_p50"],
            "tokio": r["tokio_p50"],
        }

    zmqrs_latency = parse_latency_table(text, "zmqrs_latency_tcp")
    for r in zmqrs_latency:
        if r["size"] in lat:
            lat[r["size"]]["zmqrs"] = r["libzmq_p50"]

    max_size = 128 * 1024
    sizes = sorted(s for s in tput if s in lat and s <= max_size)

    return {"sizes": sizes, "tput": tput, "lat": lat}


def generate_svg(data: dict) -> str:
    sizes = data["sizes"]
    tput = data["tput"]
    lat = data["lat"]
    n = len(sizes)

    svg_w, svg_h = 850, 520
    x_left, x_right = 90, 760
    plot_w = x_right - x_left

    t1_y_top, t1_y_bot = 35, 248
    t1_h = t1_y_bot - t1_y_top

    t2_y_top, t2_y_bot = 304, 464
    t2_h = t2_y_bot - t2_y_top

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    msg_max = 16e6

    def y_msg(v):
        return t1_y_bot - (v / msg_max) * t1_h

    tput_max_gbs = 10.0

    def y_tput(v):
        return t1_y_bot - (v / tput_max_gbs) * t1_h

    lat_max = 150.0

    def y_lat(v):
        return t2_y_bot - (v / lat_max) * t2_h

    colors = {
        "libzmq": "#eab308", "compio": "#dc2626",
        "tokio": "#f97316", "zmqrs": "#2563eb",
    }
    tput_draw_order = ["zmqrs", "libzmq", "tokio", "compio"]
    lat_draw_order = ["libzmq", "tokio", "zmqrs", "compio"]
    mid_x = (x_left + x_right) / 2

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    # ── TOP PANEL: THROUGHPUT ──────────────────────────────────────

    L.append(
        f'  <text x="{mid_x}" y="18" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput — TCP loopback (msg/s, higher is better)</text>"
    )

    for v_m in [4, 8, 12, 16]:
        yy = y_msg(v_m * 1e6)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">{v_m}M</text>'
        )

    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{t1_y_top}" x2="{x:.1f}" y2="{t1_y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    # Right-axis gridlines (GB/s, linear, dashed)
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

    L.append(
        f'  <line x1="{x_left}" y1="{t1_y_top}" x2="{x_left}" y2="{t1_y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_right}" y1="{t1_y_top}" x2="{x_right}" y2="{t1_y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{t1_y_bot}" x2="{x_right}" y2="{t1_y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t1_mid = (t1_y_top + t1_y_bot) / 2
    L.append(
        f'  <text x="40" y="{t1_mid:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t1_mid:.1f})">msg/s</text>'
    )
    L.append(
        f'  <text x="812" y="{t1_mid:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#6b7280" font-size="10" font-weight="600"'
        f' transform="rotate(90,812,{t1_mid:.1f})">throughput</text>'
    )

    # Dashed throughput (GB/s) lines
    for name in tput_draw_order:
        idxs = [i for i in range(n) if name in tput[sizes[i]]]
        pts = " ".join(
            f"{xs[i]:.1f},{y_tput(tput[sizes[i]][name][1]):.1f}" for i in idxs
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    # Solid msg/s lines with dots
    for name in tput_draw_order:
        idxs = [i for i in range(n) if name in tput[sizes[i]]]
        pts = " ".join(
            f"{xs[i]:.1f},{y_msg(tput[sizes[i]][name][0]):.1f}" for i in idxs
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i in idxs:
            yy = y_msg(tput[sizes[i]][name][0])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # X-axis labels (top panel)
    for i, s in enumerate(sizes):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{t1_y_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(s)}</text>'
        )

    # ── BOTTOM PANEL: LATENCY ─────────────────────────────────────

    L.append(
        f'  <text x="{mid_x}" y="288" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency — TCP loopback (p50 µs, lower is better)</text>"
    )

    for v in [25, 50, 75, 100, 125, 150]:
        yy = y_lat(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">{v}</text>'
        )

    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{t2_y_top}" x2="{x:.1f}" y2="{t2_y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    L.append(
        f'  <line x1="{x_left}" y1="{t2_y_top}" x2="{x_left}" y2="{t2_y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{t2_y_bot}" x2="{x_right}" y2="{t2_y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t2_mid = (t2_y_top + t2_y_bot) / 2
    L.append(
        f'  <text x="40" y="{t2_mid:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t2_mid:.1f})">p50 latency (µs)</text>'
    )

    for name in lat_draw_order:
        idxs = [i for i in range(n) if name in lat[sizes[i]]]
        pts = " ".join(
            f"{xs[i]:.1f},{y_lat(lat[sizes[i]][name]):.1f}" for i in idxs
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i in idxs:
            yy = y_lat(lat[sizes[i]][name])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # X-axis labels (bottom panel only — shared x-axis via aligned gridlines)
    for i, s in enumerate(sizes):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{t2_y_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(s)}</text>'
        )

    # ── LEGEND ────────────────────────────────────────────────────

    leg_y = t2_y_bot + 40
    legend_items = [
        ("libzmq", "libzmq"), ("compio", "omq-compio"),
        ("tokio", "omq-tokio"), ("zmqrs", "zmq.rs"),
    ]
    item_w = 140
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = colors[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>'
        )
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
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

    output = repo / "doc" / "charts" / "comparison.svg"
    output.write_text(svg)
    print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
