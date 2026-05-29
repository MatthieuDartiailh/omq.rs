#!/usr/bin/env python3
"""Generate doc/charts/compression.svg -- three-panel combined chart.

Panels (top to bottom): 1 Gbps, 100 Mbps, 10 Mbps.
Each panel: dashed = msg/s log scale (left axis), solid = virtual throughput (right axis).

The bench runs at full loopback speed.  Each panel projects what throughput
would be at its link speed: effective_msgs_s = min(cpu_msgs_s, link_bytes_s / wire_bytes).
"""

import json
import math
import sys
from pathlib import Path


LINK_SPEEDS = [
    ("1g",   1_000_000_000 / 8),
    ("100m", 100_000_000 / 8),
    ("10m",  10_000_000 / 8),
]
LINK_LABELS = {"1g": "1 Gbps", "100m": "100 Mbps", "10m": "10 Mbps"}

COLORS = {
    "tcp":            "#eab308",
    "lz4+tcp":        "#60a5fa",
    "lz4+tcp+dict":   "#1d4ed8",
    "zstd+tcp":       "#f97316",
}
LABELS = {
    "tcp":            "tcp",
    "lz4+tcp":        "lz4",
    "lz4+tcp+dict":   "lz4+dict",
    "zstd+tcp":       "zstd",
}
SERIES_ORDER = ["tcp", "lz4+tcp", "lz4+tcp+dict", "zstd+tcp"]


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def _fmt_tput(mb):
    if mb >= 1024:
        v = mb / 1024
        return f"{v:.1f} GB/s" if v < 10 else f"{v:.0f} GB/s"
    if mb >= 10:
        return f"{mb:.0f} MB/s"
    return f"{mb:.1f} MB/s"


def _tput_ticks(max_mb):
    if max_mb <= 0:
        return [1]
    candidates = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    for step in candidates:
        n_ticks = max_mb / step
        if 3 <= n_ticks <= 8:
            return [step * i for i in range(1, int(max_mb / step) + 1)]
    step = candidates[-1]
    return [step * i for i in range(1, int(max_mb / step) + 1)]


def _log_ticks(data_min, data_max):
    """Return (axis_min, axis_max, tick_values) for a log-scale axis.

    Axis endpoints use the 1-2-5 sequence so the top is at most ~2x
    above data_max.  Labeled gridlines are at decade boundaries only.
    """
    if data_min <= 0:
        data_min = 1
    if data_max <= data_min:
        data_max = data_min * 10

    steps = [1, 2, 5]

    def prev_125(v):
        exp = math.floor(math.log10(v))
        for s in reversed(steps):
            if s * 10 ** exp <= v:
                return s * 10 ** exp
        return 10 ** exp

    def next_125(v):
        exp = math.floor(math.log10(v))
        for s in steps:
            if s * 10 ** exp >= v:
                return s * 10 ** exp
        return 10 ** (exp + 1)

    lo = prev_125(data_min)
    hi = next_125(data_max)

    axis_min = math.log10(lo)
    axis_max = math.log10(hi)

    ticks = []
    for e in range(math.floor(axis_min), math.ceil(axis_max) + 1):
        for s in [1, 2, 5]:
            v = s * 10 ** e
            if lo <= v <= hi:
                ticks.append(v)

    return axis_min, axis_max, ticks


def load_raw_data(jsonl_path: Path, dict_size: int | None = None) -> dict:
    """Load the latest run and return per-series raw data (cpu_msgs_s, wire_bytes, msg_size).

    dict_size: if set, only include dict rows whose dict_size field matches.
    """
    all_rows = []
    for line in jsonl_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r["pattern"] in ("compression_json", "compression_json_dict"):
            all_rows.append(r)

    if not all_rows:
        print("ERROR: no compression rows found", file=sys.stderr)
        sys.exit(1)

    all_rows.sort(key=lambda r: r["run_id"])
    latest_id = all_rows[-1]["run_id"]
    selected = [r for r in all_rows if r["run_id"] == latest_id]
    print(f"Using {len(selected)} rows from run {latest_id}", file=sys.stderr)

    if dict_size is not None:
        selected = [
            r for r in selected
            if r["pattern"] != "compression_json_dict"
            or r.get("dict_size") == dict_size
        ]
        print(f"  filtered to {len(selected)} rows with dict_size={dict_size}",
              file=sys.stderr)

    sizes_set = set()
    series = {}
    for r in selected:
        transport = r["transport"]
        is_dict = r["pattern"] == "compression_json_dict"
        if is_dict and transport == "zstd+tcp":
            key = "zstd+tcp"
        elif is_dict:
            key = f"{transport}+dict"
        else:
            key = transport
        sizes_set.add(r["msg_size"])
        series.setdefault(key, {})[r["msg_size"]] = {
            "cpu_msgs_s": r["msgs_s"],
            "msg_size": r["msg_size"],
            "wire_bytes": r.get("wire_bytes", r["msg_size"]),
        }

    return {"sizes": sorted(sizes_set), "series": series}


def project(raw: dict, link_bytes_s: float) -> dict:
    """Project throughput at a given link speed."""
    series = {}
    for key, size_data in raw["series"].items():
        series[key] = {}
        for sz, d in size_data.items():
            wire = d["wire_bytes"]
            cpu = d["cpu_msgs_s"]
            wire_limited = link_bytes_s / max(wire, 1)
            eff_msgs_s = min(cpu, wire_limited)
            virt_mbps = eff_msgs_s * d["msg_size"] / 1_000_000
            series[key][sz] = {
                "msgs_s": eff_msgs_s,
                "virt_gbps": virt_mbps / 1024,
            }
    return {"sizes": raw["sizes"], "series": series}


def generate_svg(
    panels: dict[str, dict],
    tput_caps: dict[str, float] | None = None,
    msgs_ranges: dict[str, tuple[float, float]] | None = None,
    dict_size_label: str | None = None,
) -> str:
    links = [label for label, _ in LINK_SPEEDS if label in panels]
    if not links:
        print("ERROR: no panel data", file=sys.stderr)
        sys.exit(1)

    n_panels = len(links)
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    panel_h = 220
    panel_gap = 70
    top_margin = 30
    x_label_space = 20
    legend_h = 50

    bottom_pad = 40
    right_pad = 15
    svg_h = (top_margin + n_panels * (panel_h + x_label_space)
             + (n_panels - 1) * panel_gap + legend_h + bottom_pad)
    svg_w = 850 + right_pad
    mid_x = (x_left + x_right) / 2

    all_sizes = set()
    for d in panels.values():
        all_sizes.update(d["sizes"])
    sizes = sorted(all_sizes)
    n = len(sizes)
    xs = [x_left + i * plot_w / (n - 1) for i in range(n)] if n > 1 else [mid_x]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')
    L.append(
        f'  <text x="{mid_x}" y="16" text-anchor="middle" fill="#111827"'
        f' font-size="12" font-weight="700">'
        f'Compression transports: structured JSON, PUSH/PULL, 2-thread (omq-compio)'
        f'{f" — dict {dict_size_label}" if dict_size_label else ""}</text>'
    )

    last_x_label_y = 0

    for pi, link in enumerate(links):
        data = panels[link]
        series = data["series"]
        y_top = (top_margin + pi * (panel_h + x_label_space + panel_gap) + 25)
        y_bot = y_top + panel_h
        plot_h = y_bot - y_top

        if msgs_ranges and link in msgs_ranges:
            axis_min, axis_max, msg_ticks = _log_ticks(
                msgs_ranges[link][0], msgs_ranges[link][1])
        else:
            all_msgs = [d["msgs_s"] for s in series.values()
                        for d in s.values() if d["msgs_s"] > 0]
            msg_min = min(all_msgs) if all_msgs else 1
            msg_max = max(all_msgs) if all_msgs else 1e6
            axis_min, axis_max, msg_ticks = _log_ticks(msg_min, msg_max)

        if tput_caps and link in tput_caps:
            tput_max = tput_caps[link] / 1024
            tput_max_mb = int(tput_max * 1024)
        else:
            max_virt = max(
                (d["virt_gbps"] for s in series.values() for d in s.values()),
                default=0.001,
            )
            tput_max_mb = int(math.ceil(max_virt * 1024 / 10) * 10)
            if tput_max_mb < 10:
                tput_max_mb = int(math.ceil(max_virt * 1024))
            tput_max = tput_max_mb / 1024

        def y_msg(v, _bot=y_bot, _h=plot_h, _lmin=axis_min, _lmax=axis_max):
            if v <= 0:
                return _bot
            lv = math.log10(v)
            frac = max(0, min(1, (lv - _lmin) / (_lmax - _lmin)))
            return _bot - frac * _h

        def y_tput(v, _bot=y_bot, _h=plot_h, _max=tput_max):
            return _bot - (v / _max) * _h

        link_label = LINK_LABELS.get(link, link)
        L.append(
            f'  <text x="{mid_x:.1f}" y="{y_top - 10}" text-anchor="middle"'
            f' fill="#111827" font-size="12" font-weight="700">'
            f'{link_label}</text>'
        )

        # Left-axis gridlines (msg/s, log scale)
        for val in msg_ticks:
            yy = y_msg(val)
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
                f' stroke="#e5e7eb" stroke-width="1"/>'
            )
            L.append(
                f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
                f' dominant-baseline="middle" fill="#374151" font-size="9">'
                f'{_fmt_y_rate(val)}</text>'
            )

        # Right-axis gridlines (virtual throughput)
        ticks = _tput_ticks(tput_max_mb)
        for mb in ticks:
            v = mb / 1024
            yy = y_tput(v)
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
                f' stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>'
            )
            L.append(
                f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
                f' dominant-baseline="middle" fill="#6b7280" font-size="9">'
                f'{_fmt_tput(mb)}</text>'
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
            L.append(
                f'  <line x1="{axis_line}" stroke="#9ca3af" stroke-width="1.5"/>'
            )

        # X-axis labels
        last_x_label_y = y_bot + 14
        for i, s in enumerate(sizes):
            L.append(
                f'  <text x="{xs[i]:.1f}" y="{last_x_label_y}" text-anchor="middle"'
                f' fill="#374151" font-size="8">{fmt_size(s)}</text>'
            )

        # Axis titles
        mid_y = (y_top + y_bot) / 2
        L.append(
            f'  <text x="40" y="{mid_y:.1f}" text-anchor="middle"'
            f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
            f' transform="rotate(-90,40,{mid_y:.1f})">msg/s (log)</text>'
        )

        # Plot lines
        present = [k for k in SERIES_ORDER if k in series]

        # Dashed: msg/s (log)
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n) if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_msg(d[s]['msgs_s']):.1f}" for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none" stroke="{COLORS[name]}"'
                f' stroke-width="1.5" stroke-dasharray="5,3"/>'
            )

        # Solid: virtual throughput with dots
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n) if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_tput(d[s]['virt_gbps']):.1f}" for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none" stroke="{COLORS[name]}"'
                f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
            )
            for i, s in active:
                yy = y_tput(d[s]["virt_gbps"])
                L.append(
                    f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="2.5"'
                    f' fill="{COLORS[name]}" stroke="white" stroke-width="1"/>'
                )

    # Legend below last panel's x-axis labels
    leg_y1 = last_x_label_y + 18
    leg_y2 = leg_y1 + 12
    all_present = []
    for link in links:
        for k in SERIES_ORDER:
            if k in panels[link]["series"] and k not in all_present:
                all_present.append(k)

    n_items = len(all_present)
    item_w = plot_w / max(n_items, 1)
    for i, name in enumerate(all_present):
        lx = x_left + i * item_w
        c = COLORS[name]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y1}" x2="{lx + 14:.0f}" y2="{leg_y1}"'
            f' stroke="{c}" stroke-width="1.5" stroke-dasharray="4,3"/>'
        )
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y2}" x2="{lx + 14:.0f}" y2="{leg_y2}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <text x="{lx + 18:.0f}" y="{leg_y1 + 4}" fill="#374151" font-size="10"'
            f' font-weight="500">{LABELS[name]}</text>'
        )

    footer_y = leg_y2 + 18
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f'dashed = msg/s log (left) · solid = virtual throughput (right)</text>'
    )
    L.append("</svg>")

    return "\n".join(L) + "\n"


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dict-size", type=int, default=None,
                        help="filter dict rows to this dict_size (bytes)")
    parser.add_argument("--tput-1g", type=float, default=None,
                        help="fixed throughput cap for 1 Gbps panel (MB/s)")
    parser.add_argument("--tput-100m", type=float, default=None,
                        help="fixed throughput cap for 100 Mbps panel (MB/s)")
    parser.add_argument("--tput-10m", type=float, default=None,
                        help="fixed throughput cap for 10 Mbps panel (MB/s)")
    parser.add_argument("--msgs-1g", type=str, default=None,
                        help="fixed msg/s range for 1 Gbps panel (min,max)")
    parser.add_argument("--msgs-100m", type=str, default=None,
                        help="fixed msg/s range for 100 Mbps panel (min,max)")
    parser.add_argument("--msgs-10m", type=str, default=None,
                        help="fixed msg/s range for 10 Mbps panel (min,max)")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parent.parent
    jsonl = repo / "omq-compio" / "benches" / "results_compression.jsonl"

    if not jsonl.exists():
        print(f"ERROR: {jsonl} not found. Run the compression bench first.",
              file=sys.stderr)
        sys.exit(1)

    raw = load_raw_data(jsonl, dict_size=args.dict_size or 2048)

    panels = {}
    for label, link_bytes_s in LINK_SPEEDS:
        panels[label] = project(raw, link_bytes_s)

    default_caps = {"1g": 1000, "100m": 400, "10m": 60}
    tput_caps = {
        "1g": args.tput_1g if args.tput_1g is not None else default_caps["1g"],
        "100m": args.tput_100m if args.tput_100m is not None else default_caps["100m"],
        "10m": args.tput_10m if args.tput_10m is not None else default_caps["10m"],
    }

    msgs_ranges = {}
    for key, attr in [("1g", "msgs_1g"), ("100m", "msgs_100m"), ("10m", "msgs_10m")]:
        val = getattr(args, attr)
        if val:
            lo, hi = val.split(",")
            msgs_ranges[key] = (float(lo), float(hi))

    ds = args.dict_size or 2048
    if ds >= 1024:
        dict_size_label = f"{ds // 1024} KiB"
    else:
        dict_size_label = f"{ds} B"

    svg = generate_svg(panels, tput_caps, msgs_ranges or None, dict_size_label)
    output = repo / "doc" / "charts" / f"compression_{ds}.svg"
    output.write_text(svg)
    print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
