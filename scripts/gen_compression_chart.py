#!/usr/bin/env python3
"""Generate doc/charts/compression.svg -- four-panel combined chart.

Panels (top to bottom): 10 Gbps, 1 Gbps, 100 Mbps, 10 Mbps.
Each panel: dashed = CPU % (left axis), solid = virtual throughput (right axis).

The bench runs at full loopback speed.  Each panel projects what throughput
would be at its link speed: effective_msgs_s = min(cpu_msgs_s, link_bytes_s / wire_bytes).
CPU % is projected proportionally when wire-limited.
"""

import json
import math
import os
import sys
from pathlib import Path


LINK_SPEEDS = [
    ("10g",  10_000_000_000 / 8),
    ("1g",   1_000_000_000 / 8),
    ("100m", 100_000_000 / 8),
    ("10m",  10_000_000 / 8),
]
LINK_LABELS = {"10g": "10 Gbps", "1g": "1 Gbps", "100m": "100 Mbps", "10m": "10 Mbps"}

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


def _fmt_cpu(val):
    return f"{val:g}%"


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


def _cpu_ticks(data_max):
    """Return (axis_max, tick_values) for a linear 0-based CPU% axis."""
    if data_max <= 0:
        data_max = 100
    candidates = [50, 100, 200, 400, 500, 800, 1000]
    ceil = data_max
    for c in candidates:
        if c >= data_max:
            ceil = c
            break
    else:
        ceil = math.ceil(data_max / 100) * 100
    step = 50 if ceil <= 400 else 100
    ticks = list(range(step, int(ceil) + 1, step))
    return ceil, ticks


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
            or r.get("dict_size", dict_size) == dict_size
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
        elapsed = r.get("elapsed", 0)
        cpu_time = r.get("cpu_time", 0)
        cpu_pct = (cpu_time / elapsed * 100.0) if elapsed > 0 else 0.0
        series.setdefault(key, {})[r["msg_size"]] = {
            "cpu_msgs_s": r["msgs_s"],
            "cpu_pct": cpu_pct,
            "msg_size": r["msg_size"],
            "wire_bytes": r.get("wire_bytes", r["msg_size"]),
        }

    return {"sizes": sorted(sizes_set), "series": series}


def project(raw: dict, link_bytes_s: float) -> dict:
    """Project throughput and CPU% at a given link speed."""
    series = {}
    for key, size_data in raw["series"].items():
        series[key] = {}
        for sz, d in size_data.items():
            wire = d["wire_bytes"]
            cpu = d["cpu_msgs_s"]
            wire_limited = link_bytes_s / max(wire, 1)
            eff_msgs_s = min(cpu, wire_limited)
            virt_mbps = eff_msgs_s * d["msg_size"] / 1_000_000
            ratio = eff_msgs_s / cpu if cpu > 0 else 0
            series[key][sz] = {
                "msgs_s": eff_msgs_s,
                "virt_mbps": virt_mbps,
                "cpu_pct": d["cpu_pct"] * ratio,
            }
    return {"sizes": raw["sizes"], "series": series}


def generate_svg(
    panels: dict[str, dict],
    tput_ranges: dict[str, tuple[float, float]] | None = None,
    dict_size_label: str | None = None,
    backend: str = "compio",
    hw_label: str | None = None,
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
    top_margin = 44 if hw_label else 30
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
        f'Compression transports: structured JSON, PUSH/PULL, {THREAD_MODELS[backend]} (omq-{backend})'
        f'{f", dict {dict_size_label}" if dict_size_label else ""}</text>'
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="30" text-anchor="middle" fill="#9ca3af"'
            f' font-size="10">{hw_label}</text>'
        )

    last_x_label_y = 0

    for pi, link in enumerate(links):
        data = panels[link]
        series = data["series"]
        y_top = (top_margin + pi * (panel_h + x_label_space + panel_gap) + 25)
        y_bot = y_top + panel_h
        plot_h = y_bot - y_top

        cpu_ceil = 400
        cpu_ticks = [100, 200, 300, 400]

        if tput_ranges and link in tput_ranges:
            virt_min, virt_max = tput_ranges[link]
        else:
            all_virt_mb = [d["virt_mbps"] for s in series.values()
                           for d in s.values() if d["virt_mbps"] > 0]
            virt_min = min(all_virt_mb) if all_virt_mb else 1
            virt_max = max(all_virt_mb) if all_virt_mb else 100
        tp_min, tp_max, tp_ticks = _log_ticks(max(virt_min, 1), max(virt_max, 10))
        if tput_ranges and link in tput_ranges:
            tp_max = math.log10(tput_ranges[link][1])
            tp_ticks = [t for t in tp_ticks if t <= tput_ranges[link][1]]

        def y_cpu(v, _bot=y_bot, _h=plot_h, _ceil=cpu_ceil):
            frac = max(0, min(1, v / _ceil))
            return _bot - frac * _h

        def y_tput(v, _bot=y_bot, _h=plot_h, _lmin=tp_min, _lmax=tp_max):
            if v <= 0:
                return _bot
            lv = math.log10(v)
            frac = max(0, min(1, (lv - _lmin) / (_lmax - _lmin)))
            return _bot - frac * _h

        link_label = LINK_LABELS.get(link, link)
        L.append(
            f'  <text x="{mid_x:.1f}" y="{y_top - 10}" text-anchor="middle"'
            f' fill="#111827" font-size="12" font-weight="700">'
            f'{link_label}</text>'
        )

        # Left-axis gridlines (CPU %, linear)
        for val in cpu_ticks:
            yy = y_cpu(val)
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
                f' stroke="#e5e7eb" stroke-width="1"/>'
            )
            L.append(
                f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
                f' dominant-baseline="middle" fill="#374151" font-size="9">'
                f'{_fmt_cpu(val)}</text>'
            )

        # Right-axis gridlines (virtual throughput, log scale)
        for mb in tp_ticks:
            yy = y_tput(mb)
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
            f' transform="rotate(-90,40,{mid_y:.1f})">CPU %</text>'
        )

        # Plot lines
        present = [k for k in SERIES_ORDER if k in series]

        # Dashed: CPU %
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n) if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_cpu(d[s]['cpu_pct']):.1f}" for i, s in active
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
                f"{xs[i]:.1f},{y_tput(d[s]['virt_mbps']):.1f}" for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none" stroke="{COLORS[name]}"'
                f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
            )
            for i, s in active:
                yy = y_tput(d[s]["virt_mbps"])
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
        f'dashed = CPU % (left) · solid = virtual throughput log (right)</text>'
    )
    L.append("</svg>")

    return "\n".join(L) + "\n"


THREAD_MODELS = {
    "compio": "2-process",
    "tokio": "2-process",
}


def detect_hardware() -> str | None:
    """Read CPU model, core count, governor, and turbo state."""
    try:
        cpu = None
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                break
        cores = os.cpu_count()
        if cpu and cores:
            label = f"{cpu}, {cores} cores"
            extras = []
            try:
                gov = open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read().strip()
                if gov == "performance":
                    extras.append("performance governor")
            except OSError:
                pass
            for path, off_val in [
                ("/sys/devices/system/cpu/intel_pstate/no_turbo", "1"),
                ("/sys/devices/system/cpu/cpufreq/boost", "0"),
            ]:
                try:
                    if open(path).read().strip() == off_val:
                        extras.append("turbo off")
                    break
                except OSError:
                    continue
            if not extras:
                hw_extras = os.environ.get("OMQ_HW_EXTRAS")
                if hw_extras:
                    extras.extend(hw_extras.split(","))
            if extras:
                label += ", " + ", ".join(extras)
            return label
    except OSError:
        pass
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["compio", "tokio"], default="compio",
                        help="which backend's results to chart (default: compio)")
    parser.add_argument("--dict-size", type=int, default=None,
                        help="filter dict rows to this dict_size (bytes)")
    parser.add_argument("--tput-10g", type=str, default=None,
                        help="throughput range for 10 Gbps panel (min,max MB/s)")
    parser.add_argument("--tput-1g", type=str, default=None,
                        help="throughput range for 1 Gbps panel (min,max MB/s)")
    parser.add_argument("--tput-100m", type=str, default=None,
                        help="throughput range for 100 Mbps panel (min,max MB/s)")
    parser.add_argument("--tput-10m", type=str, default=None,
                        help="throughput range for 10 Mbps panel (min,max MB/s)")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parent.parent
    cache_dir = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
    jsonl = cache_dir / f"results_compression_{args.backend}.jsonl"

    if not jsonl.exists():
        print(f"ERROR: {jsonl} not found. Run the compression bench first.",
              file=sys.stderr)
        sys.exit(1)

    raw = load_raw_data(jsonl, dict_size=args.dict_size or 2048)

    panels = {}
    for label, link_bytes_s in LINK_SPEEDS:
        panels[label] = project(raw, link_bytes_s)

    def parse_range(val, default):
        if val is not None:
            lo, hi = val.split(",")
            return (float(lo), float(hi))
        return default

    default_tput = {"10g": (10, 4000), "1g": (5, 2000), "100m": (1, 400), "10m": (1, 40)}

    tput_ranges = {
        "10g": parse_range(args.tput_10g, default_tput["10g"]),
        "1g": parse_range(args.tput_1g, default_tput["1g"]),
        "100m": parse_range(args.tput_100m, default_tput["100m"]),
        "10m": parse_range(args.tput_10m, default_tput["10m"]),
    }

    ds = args.dict_size or 2048
    if ds >= 1024:
        dict_size_label = f"{ds // 1024} KiB"
    else:
        dict_size_label = f"{ds} B"

    svg = generate_svg(panels, tput_ranges, dict_size_label,
                       backend=args.backend, hw_label=detect_hardware())
    output = repo / "doc" / "charts" / "compression" / f"{args.backend}_{ds}.svg"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(svg)
    print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
