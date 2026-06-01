#!/usr/bin/env ruby
# frozen_string_literal: true

# Benchmark regression report and BENCHMARKS.md table updater.
# Reads omq-{compio,tokio}/benches/results.jsonl.
#
# Usage:
#   ruby scripts/bench_report.rb                      # latest vs previous (both backends)
#   ruby scripts/bench_report.rb --backend compio     # compio only
#   ruby scripts/bench_report.rb --runs 5             # compare latest vs oldest-of-5
#   ruby scripts/bench_report.rb --threshold 10       # 10% noise band
#   ruby scripts/bench_report.rb --all                # show all measurements
#   ruby scripts/bench_report.rb --pattern push_pull  # filter to one pattern
#   ruby scripts/bench_report.rb --exclude-run ID     # drop a noisy run (repeatable)
#   ruby scripts/bench_report.rb --update-benchmarks  # regenerate BENCHMARKS.md tables

require 'optparse'
require_relative 'lib/bench_helpers'

ROOT            = File.expand_path('..', __dir__)
BENCHMARKS_PATH = File.join(ROOT, 'BENCHMARKS.md')
JSONL_PATH = {
  'compio' => File.join(ROOT, 'omq-compio', 'benches', 'results.jsonl'),
  'tokio'  => File.join(ROOT, 'omq-tokio',  'benches', 'results.jsonl'),
}.freeze

# ── formatting helpers (local, not shared) ────────────────────────────────────

def format_mbps_report(v)
  return '--' unless v && v > 0
  v >= 1_000 ? '%.1f GB/s' % (v / 1_000.0) : '%.1f MB/s' % v
end

# ── options ───────────────────────────────────────────────────────────────────

options = {
  backends:           %w[compio tokio],
  runs:               2,
  threshold:          10.0,
  all:                false,
  pattern:            nil,
  exclude_runs:       [],
  update_benchmarks:  false,
}

OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/bench_report.rb [options]'
  o.on('--backend BACKEND',      'Show only "compio" or "tokio"')       { |v| options[:backends]          = [v] }
  o.on('--runs N', Integer,      'Runs to compare (default 2)')         { |v| options[:runs]              = v }
  o.on('--threshold PCT', Float, 'Noise band % (default 5)')            { |v| options[:threshold]         = v }
  o.on('--all',                  'Show all measurements')               { options[:all]                   = true }
  o.on('--pattern NAME',         'Filter to one pattern')               { |v| options[:pattern]           = v }
  o.on('--exclude-run RUN_ID',   'Exclude a run_id (repeatable)')       { |v| options[:exclude_runs]      << v }
  o.on('--update-benchmarks',    'Regenerate tables in BENCHMARKS.md')  { options[:update_benchmarks]     = true }
end.parse!

# ── load results ─────────────────────────────────────────────────────────────

rows_by_backend = JSONL_PATH.filter_map { |b, p|
  next unless options[:backends].include?(b)
  [b, BenchHelpers.load_jsonl(p, exclude_runs: options[:exclude_runs])]
}.to_h

# ── --update-benchmarks ─────────────────────────────────────────────────────

if options[:update_benchmarks]
  THROUGHPUT_CELL = ->(row) {
    next '—' unless row
    [BenchHelpers.format_si(row[:msgs_s]), BenchHelpers.format_mbps(row[:mbps])].compact.join(' / ')
  }
  MBPS_CELL     = ->(row) { row ? (BenchHelpers.format_mbps(row[:mbps]) || '—') : '—' }

  CORE = %w[inproc ipc tcp ws]

  TABLE_DEFS = [
    { stem: 'push_pull_1peer',        pattern: 'push_pull',        peers: 1,
      transports: { 'compio' => %w[inproc inproc-mt ipc tcp ws], 'tokio' => CORE } },
    { stem: 'push_pull_8peer',        pattern: 'push_pull',        peers: 8 },
    { stem: 'push_pull_fanout_8peer', pattern: 'push_pull_fanout', peers: 8 },
    { stem: 'req_rep',                pattern: 'req_rep',           peers: 1 },
    { stem: 'pub_sub',                pattern: 'pub_sub',           peers: 3 },
    { stem: 'router_dealer',          pattern: 'router_dealer',     peers: 3 },
    { stem: 'pair',                   pattern: 'pair',              peers: 1 },
  ].freeze

  bm = File.read(BENCHMARKS_PATH)

  TABLE_DEFS.each do |d|
    %w[compio tokio].each do |backend|
      ts = d.fetch(:transports, CORE)
      ts = ts[backend] if ts.is_a?(Hash)
      rows = rows_by_backend[backend] || []

      lookup = ->(transport, sz) {
        BenchHelpers.latest_row(rows, pattern: d[:pattern], transport: transport,
                                      peers: d[:peers], msg_size: sz)
      }

      empty = "no #{d[:pattern]} #{backend} data"
      content = BenchHelpers.build_size_table(
        columns: ts, cell_fmt: d.fetch(:cell_fmt, THROUGHPUT_CELL),
        lookup: lookup, empty_msg: empty, sizes: BenchHelpers::TABLE_SIZES,
      )
      bm = BenchHelpers.replace_block(bm, "#{d[:stem]}_#{backend}", content)
    end
  end

  build_latency_table = ->(pattern, empty_msg) {
    transports = CORE.select do |t|
      BenchHelpers::TABLE_SIZES.any? do |s|
        BenchHelpers.latest_row(rows_by_backend['compio'] || [], pattern: pattern, transport: t, peers: 1, msg_size: s) ||
          BenchHelpers.latest_row(rows_by_backend['tokio'] || [], pattern: pattern, transport: t, peers: 1, msg_size: s)
      end
    end
    sizes = BenchHelpers::TABLE_SIZES.select do |s|
      transports.any? do |t|
        BenchHelpers.latest_row(rows_by_backend['compio'] || [], pattern: pattern, transport: t, peers: 1, msg_size: s) ||
          BenchHelpers.latest_row(rows_by_backend['tokio'] || [], pattern: pattern, transport: t, peers: 1, msg_size: s)
      end
    end

    if transports.empty?
      "(#{empty_msg})\n"
    else
      out = +""
      out << "| transport | size | compio p50 | compio p99 | tokio p50 | tokio p99 |\n"
      out << "|---|---|---|---|---|---|\n"
      transports.each do |t|
        sizes.each do |sz|
          c  = BenchHelpers.latest_row(rows_by_backend['compio'] || [], pattern: pattern, transport: t, peers: 1, msg_size: sz)
          tk = BenchHelpers.latest_row(rows_by_backend['tokio']  || [], pattern: pattern, transport: t, peers: 1, msg_size: sz)
          next if c.nil? && tk.nil?
          out << "| #{t} | #{BenchHelpers.size_label(sz)} |"
          out << " #{BenchHelpers.format_us(c&.fetch(:p50_us, nil))} |"
          out << " #{BenchHelpers.format_us(c&.fetch(:p99_us, nil))} |"
          out << " #{BenchHelpers.format_us(tk&.fetch(:p50_us, nil))} |"
          out << " #{BenchHelpers.format_us(tk&.fetch(:p99_us, nil))} |\n"
        end
      end
      out << "\n"
      out
    end
  }

  bm = BenchHelpers.replace_block(bm, 'latency_percentiles',
    build_latency_table.call('latency', 'no latency data'))
  bm = BenchHelpers.replace_block(bm, 'client_server_latency_percentiles',
    build_latency_table.call('client_server_latency', 'no client_server_latency data'))

  # mechanism_frame — end-to-end mechanism cost over TCP (compio only)
  begin
    mechanisms = %w[NULL CURVE BLAKE3ZMQ]
    mech_rows = rows_by_backend['compio'] || []
    lookup = ->(mech, sz) {
      BenchHelpers.latest_row(mech_rows, pattern: 'mechanism', transport: mech, peers: 1, msg_size: sz)
    }
    content = BenchHelpers.build_size_table(
      columns: mechanisms, cell_fmt: MBPS_CELL, lookup: lookup,
      empty_msg: "no mechanism data — run: cargo bench -p omq-compio --bench mechanism --features 'curve blake3zmq'",
      col_align: '---:', sizes: BenchHelpers::TABLE_SIZES,
    )
    bm = BenchHelpers.replace_block(bm, 'mechanism_frame', content)
  end

  File.write(BENCHMARKS_PATH, bm)
  run_counts = rows_by_backend.transform_values { |rows| rows.map { |r| r[:run_id] }.uniq.size }
  puts "Updated #{BENCHMARKS_PATH} (#{run_counts.map { |b, n| "#{b}: #{n} runs" }.join(', ')})"
  exit 0
end

# ── regression report ────────────────────────────────────────────────────────

RED    = "\e[31m"
GREEN  = "\e[32m"
YELLOW = "\e[33m"
DIM    = "\e[2m"
BOLD   = "\e[1m"
RESET  = "\e[0m"

threshold = options[:threshold]
any_output = false

options[:backends].each do |backend|
  rows = rows_by_backend[backend] || []
  rows = rows.select { |r| r[:pattern] == options[:pattern] } if options[:pattern]

  all_run_ids = rows.map { |r| r[:run_id] }.uniq
  run_ids = all_run_ids.last(options[:runs])

  if run_ids.size < 2
    warn "#{backend}: need at least 2 runs to compare, found #{run_ids.size} (#{all_run_ids.size} total)"
    next
  end

  base_run   = run_ids.first
  latest_run = run_ids.last

  by_key = Hash.new { |h, k| h[k] = {} }
  rows.each do |r|
    next unless run_ids.include?(r[:run_id])
    key = [r[:pattern], r[:transport], r[:peers], r[:msg_size]]
    by_key[key][r[:run_id]] = r
  end

  regressions  = []
  improvements = []
  trends       = []
  stable_count = 0

  by_key.sort.each do |key, runs|
    base   = runs[base_run]
    latest = runs[latest_run]
    next unless base && latest

    pattern, transport, peers, msg_size = key
    peer_label = "#{peers} peer#{'s' if peers > 1}"

    [:msgs_s, :mbps].each do |metric|
      old_val = base[metric]
      new_val = latest[metric]
      next unless old_val && old_val != 0

      fmt   = metric == :msgs_s ? BenchHelpers.method(:format_si) : method(:format_mbps_report)
      delta = ((new_val - old_val) / old_val.to_f * 100).round(1)
      row   = { pattern: pattern, transport: transport, peers: peer_label,
                size: BenchHelpers.size_label(msg_size), metric: metric,
                old: fmt.(old_val), new: fmt.(new_val), delta: delta }

      if delta <= -threshold
        regressions << row
      elsif delta >= threshold
        improvements << row
      else
        values = run_ids.map { |id| runs[id]&.fetch(metric, nil) }.compact
        if values.size >= 3
          if values.each_cons(2).all? { |a, b| b < a }
            trends << row.merge(direction: :down, runs: values.size)
          elsif values.each_cons(2).all? { |a, b| b > a }
            trends << row.merge(direction: :up, runs: values.size)
          else
            stable_count += 1
          end
        else
          stable_count += 1
        end
      end
    end
  end

  total      = regressions.size + improvements.size + trends.size + stable_count
  span_label = run_ids.size == 2 ? "#{latest_run} vs #{base_run}" :
               "#{latest_run} vs #{base_run} (#{run_ids.size} runs)"
  span_label += " [#{all_run_ids.size} total]" if all_run_ids.size > run_ids.size

  puts "#{BOLD}=== #{backend} (#{span_label}) ===#{RESET}"
  puts
  any_output = true

  if regressions.any?
    puts "#{RED}#{BOLD}REGRESSIONS (>#{threshold}%):#{RESET}"
    regressions.each do |r|
      printf "  %-15s %-8s %-9s %-7s %-6s  %10s → %-10s  #{RED}%+.1f%%#{RESET}\n",
             r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], r[:delta]
    end
    puts
  end

  if improvements.any?
    puts "#{GREEN}#{BOLD}IMPROVEMENTS (>#{threshold}%):#{RESET}"
    improvements.each do |r|
      printf "  %-15s %-8s %-9s %-7s %-6s  %10s → %-10s  #{GREEN}%+.1f%%#{RESET}\n",
             r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], r[:delta]
    end
    puts
  end

  if trends.any?
    puts "#{YELLOW}#{BOLD}TRENDS (monotonic across #{run_ids.size} runs, within ±#{threshold}%):#{RESET}"
    trends.each do |r|
      arrow = r[:direction] == :down ? '↓' : '↑'
      printf "  %-15s %-8s %-9s %-7s %-6s  %10s → %-10s  #{YELLOW}%s %+.1f%%#{RESET}\n",
             r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], arrow, r[:delta]
    end
    puts
  end

  if regressions.empty? && improvements.empty? && trends.empty?
    puts "#{DIM}All #{total} measurements stable (±#{threshold}%)#{RESET}"
  else
    puts "#{DIM}#{total} total: #{regressions.size} regressions, " \
         "#{improvements.size} improvements, #{trends.size} trends, #{stable_count} stable (±#{threshold}%)#{RESET}"
  end
  puts

  next unless options[:all]

  puts "#{BOLD}--- full results (#{backend}) ---#{RESET}"
  by_key.sort.each do |key, runs|
    pattern, transport, peers, msg_size = key
    peer_label = "#{peers} peer#{'s' if peers > 1}"
    printf "\n  %-15s %-8s %-9s %-7s", pattern, transport, peer_label, BenchHelpers.size_label(msg_size)
    [:msgs_s, :mbps].each do |metric|
      fmt    = metric == :msgs_s ? BenchHelpers.method(:format_si) : method(:format_mbps_report)
      values = run_ids.map { |id| runs[id]&.fetch(metric, nil) }
      printf '  %-6s', metric
      values.each { |v| printf '  %10s', v ? fmt.(v) : '--' }
      base_v, latest_v = values.first, values.last
      if base_v && latest_v && base_v != 0
        delta = ((latest_v - base_v) / base_v.to_f * 100).round(1)
        color = delta <= -threshold ? RED : delta >= threshold ? GREEN : DIM
        printf "  #{color}%+.1f%%#{RESET}", delta
      end
    end
  end
  puts "\n"
end

unless any_output
  puts "#{DIM}No data found.#{RESET}"
end
