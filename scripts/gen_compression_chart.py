#!/usr/bin/env python3
"""Generate doc/charts/compression.svg from compression bench JSONL data."""

import json
import math
import sys
from collections import defaultdict
from pathlib import Path


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def load_data(jsonl_path: Path, run_prefix: str | None = None) -> dict:
    rows = []
    for line in jsonl_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r["pattern"] in ("compression_json", "compression_json_dict"):
            rows.append(r)

    rows.sort(key=lambda r: r["run_id"])
    if not rows:
        print("ERROR: no compression_json rows found", file=sys.stderr)
        sys.exit(1)

    if run_prefix:
        selected = [r for r in rows if r["run_id"].startswith(run_prefix)]
        print(f"Using {len(selected)} rows matching {run_prefix}*", file=sys.stderr)
    else:
        latest_id = rows[-1]["run_id"]
        try:
            latest_ts = int(latest_id.split("-")[1])
        except (IndexError, ValueError):
            latest_ts = 0
        selected = [r for r in rows
                    if r["run_id"].startswith("ts-")
                    and abs(int(r["run_id"].split("-")[1]) - latest_ts) < 600]
        if not selected:
            selected = rows
        print(f"Using {len(selected)} rows near {latest_id}", file=sys.stderr)

    sizes_set = set()
    series = {}

    for r in selected:
        transport = r["transport"]
        is_dict = r["pattern"] == "compression_json_dict"
        key = f"{transport}+dict" if is_dict else transport
        sizes_set.add(r["msg_size"])
        series.setdefault(key, {})[r["msg_size"]] = {
            "msgs_s": r["msgs_s"],
            "virt_gbps": r["mbps"] / 1024,
            "wire_gbps": r["wire_mbps"] / 1024,
        }

    sizes = sorted(sizes_set)
    return {"sizes": sizes, "series": series}


def generate_svg(data: dict, link_label: str = "1 Gbps link",
                 tput_max_mb: int | None = None) -> str:
    sizes = data["sizes"]
    series = data["series"]
    n = len(sizes)

    x_left, x_right = 90, 760
    y_top, y_bot = 45, 350
    svg_h = 450
    plot_w = x_right - x_left
    plot_h = y_bot - y_top

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    msg_log_min = 3.0   # log10(1k)
    msg_log_max = 6.0   # log10(1M)

    if tput_max_mb is None:
        max_virt = max(
            d["virt_gbps"]
            for s in series.values()
            for d in s.values()
        )
        tput_max_mb = int(math.ceil(max_virt * 1024 / 50) * 50)  # round up to 50 MB/s
    tput_max = tput_max_mb / 1024  # GB/s

    def y_msg(v):
        if v <= 0:
            return y_bot
        log_v = math.log10(v)
        frac = (log_v - msg_log_min) / (msg_log_max - msg_log_min)
        return y_bot - max(0, frac) * plot_h

    def y_tput(v):
        return y_bot - (v / tput_max) * plot_h

    colors = {
        "tcp":            "#eab308",
        "lz4+tcp":        "#60a5fa",
        "lz4+tcp+dict":   "#1d4ed8",
        "zstd+tcp":       "#f97316",
        "zstd+tcp+dict":  "#dc2626",
    }
    labels = {
        "tcp":            "tcp",
        "lz4+tcp":        "lz4",
        "lz4+tcp+dict":   "lz4+dict",
        "zstd+tcp":       "zstd",
        "zstd+tcp+dict":  "zstd+dict",
    }
    order = ["tcp", "lz4+tcp", "lz4+tcp+dict", "zstd+tcp", "zstd+tcp+dict"]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="850" height="{svg_h}" fill="white"/>')

    # Left-axis: msg/s log scale (major)
    for exp, label in [(3, "1k"), (4, "10k"), (5, "100k"), (6, "1M")]:
        yy = y_bot - (exp - msg_log_min) / (msg_log_max - msg_log_min) * plot_h
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">{label}</text>'
        )

    # Left-axis: minor gridlines
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

    # Right-axis: virtual throughput (dashed)
    step = max(50, (tput_max_mb // 5 // 50) * 50) or 50
    for mb in range(step, tput_max_mb + 1, step):
        v = mb / 1024  # GB/s
        yy = y_tput(v)
        label = f"{mb} MB/s"
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>'
        )
        L.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#6b7280" font-size="10">'
            f'{label}</text>'
        )

    # Vertical gridlines
    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{y_top}" x2="{x:.1f}" y2="{y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    # Axes
    for axis_line in [
        f'{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}',
        f'{x_right}" y1="{y_top}" x2="{x_right}" y2="{y_bot}',
        f'{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}',
    ]:
        L.append(f'  <line x1="{axis_line}" stroke="#9ca3af" stroke-width="1.5"/>')

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
        f'  <text x="830" y="{mid_y:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#6b7280" font-size="11" font-weight="600"'
        f' transform="rotate(90,830,{mid_y:.1f})">virtual throughput</text>'
    )
    L.append(
        f'  <text x="{mid_x:.1f}" y="22" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f'Compression transports: structured JSON, {link_label} (omq-compio)</text>'
    )

    # --- Plot lines ---
    present = [k for k in order if k in series]

    # Dashed: virtual throughput
    for name in present:
        d = series[name]
        pts = " ".join(
            f"{xs[i]:.1f},{y_tput(d[sizes[i]]['virt_gbps']):.1f}"
            for i in range(n) if sizes[i] in d
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    # Solid: msg/s with dots
    for name in present:
        d = series[name]
        active = [(i, sizes[i]) for i in range(n) if sizes[i] in d]
        pts = " ".join(
            f"{xs[i]:.1f},{y_msg(d[s]['msgs_s']):.1f}" for i, s in active
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, s in active:
            yy = y_msg(d[s]["msgs_s"])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # Legend
    leg_y1 = y_bot + 38
    leg_y2 = leg_y1 + 12
    legend_xs = [90, 220, 360, 500, 630]
    for i, name in enumerate(present):
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
        f'solid = msg/s (left, log) · dashed = virtual throughput (right, linear)</text>'
    )
    L.append("</svg>")

    return "\n".join(L) + "\n"


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--link", default="1g",
                        help="Link label for title and filename suffix (e.g. 1g, 100m)")
    parser.add_argument("--run-prefix", default=None,
                        help="Run ID prefix to select (e.g. ts-177923)")
    parser.add_argument("--tput-max", type=int, default=None,
                        help="Right-axis max in MB/s (auto-detected if omitted)")
    args = parser.parse_args()

    link_labels = {"1g": "1 Gbps link", "100m": "100 Mbps link", "10g": "10 Gbps link"}
    link_label = link_labels.get(args.link, f"{args.link} link")

    repo = Path(__file__).resolve().parent.parent
    jsonl = repo / "omq-compio" / "benches" / "results.jsonl"

    if not jsonl.exists():
        print(f"ERROR: {jsonl} not found. Run the compression bench first.", file=sys.stderr)
        sys.exit(1)

    data = load_data(jsonl, run_prefix=args.run_prefix)
    svg = generate_svg(data, link_label=link_label, tput_max_mb=args.tput_max)

    output = repo / "doc" / "charts" / f"compression_{args.link}.svg"
    output.write_text(svg)
    print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
