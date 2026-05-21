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

require 'json'
require 'optparse'

ROOT            = File.expand_path('..', __dir__)
BENCHMARKS_PATH = File.join(ROOT, 'BENCHMARKS.md')
JSONL_PATH = {
  'compio' => File.join(ROOT, 'omq-compio', 'benches', 'results.jsonl'),
  'tokio'  => File.join(ROOT, 'omq-tokio',  'benches', 'results.jsonl'),
}.freeze

JSONL_PRIORITY_PATH = {
  'compio' => File.join(ROOT, 'omq-compio', 'benches', 'results_priority.jsonl'),
  'tokio'  => File.join(ROOT, 'omq-tokio',  'benches', 'results_priority.jsonl'),
}.freeze

SIZE_LABELS = {
  32      => '32 B',
  64      => '64 B',
  128     => '128 B',
  256     => '256 B',
  512     => '512 B',
  1_024   => '1 KiB',
  2_048   => '2 KiB',
  4_096   => '4 KiB',
  8_192   => '8 KiB',
  32_768  => '32 KiB',
  131_072 => '128 KiB',
}.freeze

# ── formatting helpers ─────────────────────────────────────────────────────────

def size_label(n)
  SIZE_LABELS[n] || "#{n} B"
end

def format_si(v)
  return nil unless v && v > 0
  if    v >= 1e6   then '%.2fM' % (v / 1e6)
  elsif v >= 100e3 then '%.0fk' % (v / 1e3)
  elsif v >= 1e3   then '%.1fk' % (v / 1e3)
  else                  '%.0f'  % v
  end
end

def format_mbps_short(v)
  return nil unless v && v > 0
  if    v >= 10_000 then '%.1f GB/s' % (v / 1_000.0)
  elsif v >= 1_000  then '%.2f GB/s' % (v / 1_000.0)
  elsif v >= 100    then '%.0f MB/s' % v
  elsif v >= 10     then '%.1f MB/s' % v
  else                   '%.2f MB/s' % v
  end
end

def throughput_cell(row)
  return '—' unless row
  [format_si(row[:msgs_s]), format_mbps_short(row[:mbps])].compact.join(' / ')
end

def latency_cell(row)
  return '—' unless row && row[:msgs_s] && row[:msgs_s] > 0
  us = 1_000_000.0 / row[:msgs_s]
  us_s = if us >= 100 then '%.0f µs' % us
           elsif us >= 10 then '%.1f µs' % us
           else '%.1f µs' % us
           end
  "#{us_s} (#{format_si(row[:msgs_s])})"
end

def format_us(v)
  return '—' unless v
  fv = v.to_f
  return '—' unless fv > 0
  if    fv >= 10_000 then '%.0f ms'  % (fv / 1_000.0)
  elsif fv >= 1_000  then '%.1f ms'  % (fv / 1_000.0)
  elsif fv >= 100    then '%.0f µs'  % fv
  elsif fv >= 10     then '%.1f µs'  % fv
  else                    '%.2f µs'  % fv
  end
end

def format_mbps_report(v)
  return '--' unless v && v > 0
  v >= 1_000 ? '%.1f GB/s' % (v / 1_000.0) : '%.1f MB/s' % v
end

# ── options ────────────────────────────────────────────────────────────────────

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

# ── load results ──────────────────────────────────────────────────────────────

rows_by_backend = {}
JSONL_PATH.each do |backend, path|
  next unless options[:backends].include?(backend)
  unless File.exist?(path)
    rows_by_backend[backend] = []
    next
  end
  rows_by_backend[backend] = File.readlines(path, chomp: true).filter_map do |line|
    next if line.strip.empty?
    JSON.parse(line, symbolize_names: true) rescue nil
  end
  unless options[:exclude_runs].empty?
    rows_by_backend[backend].reject! { |r| options[:exclude_runs].include?(r[:run_id]) }
  end
end

priority_rows_by_backend = {}
JSONL_PRIORITY_PATH.each do |backend, path|
  next unless options[:backends].include?(backend)
  unless File.exist?(path)
    priority_rows_by_backend[backend] = []
    next
  end
  priority_rows_by_backend[backend] = File.readlines(path, chomp: true).filter_map do |line|
    next if line.strip.empty?
    JSON.parse(line, symbolize_names: true) rescue nil
  end
  unless options[:exclude_runs].empty?
    priority_rows_by_backend[backend].reject! { |r| options[:exclude_runs].include?(r[:run_id]) }
  end
end

# ── --update-benchmarks ───────────────────────────────────────────────────────

if options[:update_benchmarks]
  # For each cell, scan backwards and return the most recent matching row.
  latest = lambda do |backend, pattern, transport, peers, msg_size|
    (rows_by_backend[backend] || []).reverse_each.find do |r|
      r[:pattern]   == pattern   &&
        r[:transport] == transport &&
        r[:peers]     == peers     &&
        r[:msg_size]  == msg_size
    end
  end

  replace_block = lambda do |text, marker, new_content|
    b = "<!-- BEGIN #{marker} -->"
    e = "<!-- END #{marker} -->"
    re = /#{Regexp.escape(b)}.*?#{Regexp.escape(e)}/m
    abort "Marker <!-- BEGIN #{marker} --> not found in BENCHMARKS.md" unless text.match?(re)
    text.sub(re, "#{b}#{new_content}#{e}")
  end

  # push_pull_1peer — per backend, push_pull, peers=1, core transports
  build_push_pull_1peer_compio = lambda do
    transports = %w[inproc inproc-mt ipc tcp]
      .select { |t| SIZE_LABELS.keys.any? { |s| latest.call('compio', 'push_pull', t, 1, s) } }
    sizes = SIZE_LABELS.keys.select { |s| transports.any? { |t| latest.call('compio', 'push_pull', t, 1, s) } }
    return "\n(no push_pull compio data)\n" if transports.empty?

    out = +"\n"
    out << "| Size | #{transports.join(' | ')} |\n"
    out << "|---|#{transports.map { '---' }.join('|')}|\n"
    sizes.each do |sz|
      cells = transports.map { |t| throughput_cell(latest.call('compio', 'push_pull', t, 1, sz)) }
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  build_push_pull_1peer_tokio = lambda do
    transports = %w[inproc ipc tcp]
      .select { |t| SIZE_LABELS.keys.any? { |s| latest.call('tokio', 'push_pull', t, 1, s) } }
    sizes = SIZE_LABELS.keys.select { |s| transports.any? { |t| latest.call('tokio', 'push_pull', t, 1, s) } }
    return "\n(no push_pull tokio data)\n" if transports.empty?

    out = +"\n"
    out << "| Size | #{transports.join(' | ')} |\n"
    out << "|---|#{transports.map { '---' }.join('|')}|\n"
    sizes.each do |sz|
      cells = transports.map { |t| throughput_cell(latest.call('tokio', 'push_pull', t, 1, sz)) }
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  # latency_percentiles — both backends, latency bench, peers=1, core transports
  build_latency_percentiles = lambda do
    transports = %w[inproc ipc tcp].select do |t|
      SIZE_LABELS.keys.any? do |s|
        latest.call('compio', 'latency', t, 1, s) || latest.call('tokio', 'latency', t, 1, s)
      end
    end
    sizes = SIZE_LABELS.keys.select do |s|
      transports.any? do |t|
        latest.call('compio', 'latency', t, 1, s) || latest.call('tokio', 'latency', t, 1, s)
      end
    end
    return "\n(no latency data)\n" if transports.empty?

    out = +"\n"
    out << "| transport | size | compio p50 | compio p99 | tokio p50 | tokio p99 |\n"
    out << "|---|---|---|---|---|---|\n"
    transports.each do |t|
      sizes.each do |sz|
        c  = latest.call('compio', 'latency', t, 1, sz)
        tk = latest.call('tokio',  'latency', t, 1, sz)
        next if c.nil? && tk.nil?
        out << "| #{t} | #{size_label(sz)} |"
        out << " #{format_us(c&.fetch(:p50_us, nil))} |"
        out << " #{format_us(c&.fetch(:p99_us, nil))} |"
        out << " #{format_us(tk&.fetch(:p50_us, nil))} |"
        out << " #{format_us(tk&.fetch(:p99_us, nil))} |\n"
      end
    end
    out << "\n"
    out
  end

  # push_pull_8peer — per backend, push_pull, peers=8, core transports
  # (no inproc-mt for 8-peer)
  build_push_pull_8peer = lambda do |backend|
    transports = %w[inproc ipc tcp]
      .select { |t| SIZE_LABELS.keys.any? { |s| latest.call(backend, 'push_pull', t, 8, s) } }
    sizes = SIZE_LABELS.keys.select { |s| transports.any? { |t| latest.call(backend, 'push_pull', t, 8, s) } }
    return "\n(no push_pull 8-peer #{backend} data)\n" if transports.empty?

    out = +"\n"
    out << "| Size | #{transports.join(' | ')} |\n"
    out << "|---|#{transports.map { '---' }.join('|')}|\n"
    sizes.each do |sz|
      cells = transports.map { |t| throughput_cell(latest.call(backend, 'push_pull', t, 8, sz)) }
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  # Generic per-backend throughput table builder.
  build_throughput_table = lambda do |pattern, peers, transports, backend, empty_msg|
    ts = transports
      .select { |t| SIZE_LABELS.keys.any? { |s| latest.call(backend, pattern, t, peers, s) } }
    sizes = SIZE_LABELS.keys.select { |s| ts.any? { |t| latest.call(backend, pattern, t, peers, s) } }
    return "\n(#{empty_msg})\n" if ts.empty?

    out = +"\n"
    out << "| Size | #{ts.join(' | ')} |\n"
    out << "|---|#{ts.map { '---' }.join('|')}|\n"
    sizes.each do |sz|
      cells = ts.map { |t| throughput_cell(latest.call(backend, pattern, t, peers, sz)) }
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  core_transports = %w[inproc ipc tcp]

  build_push_pull_fanout_8peer = lambda do |backend|
    build_throughput_table.call('push_pull_fanout', 8, core_transports, backend, "no push_pull_fanout 8-peer #{backend} data")
  end

  build_req_rep       = ->(b) { build_throughput_table.call('req_rep',        1, core_transports, b, "no req_rep #{b} data") }
  build_pub_sub       = ->(b) { build_throughput_table.call('pub_sub',        3, core_transports, b, "no pub_sub #{b} data") }
  build_router_dealer = ->(b) { build_throughput_table.call('router_dealer',  3, core_transports, b, "no router_dealer #{b} data") }
  build_pair          = ->(b) { build_throughput_table.call('pair',           1, core_transports, b, "no pair #{b} data") }

  # push_pull_priority — both backends, push_pull, peers=1, priority feature
  latest_priority = lambda do |backend, pattern, transport, peers, msg_size|
    (priority_rows_by_backend[backend] || []).reverse_each.find do |r|
      r[:pattern]   == pattern   &&
        r[:transport] == transport &&
        r[:peers]     == peers     &&
        r[:msg_size]  == msg_size
    end
  end

  build_push_pull_priority = lambda do |backend|
    transports = %w[inproc ipc tcp]
    sizes = SIZE_LABELS.keys.select do |s|
      transports.any? { |t| latest_priority.call(backend, 'push_pull', t, 1, s) }
    end
    return "\n(no push_pull priority #{backend} data — run: bench_run.rb --with-priority)\n" if sizes.empty?

    out = +"\n"
    out << "| Size | #{transports.join(' | ')} |\n"
    out << "|---|#{transports.map { '---' }.join('|')}|\n"
    sizes.each do |sz|
      cells = transports.map do |t|
        format_si(latest_priority.call(backend, 'push_pull', t, 1, sz)&.fetch(:msgs_s, nil)) || '—'
      end
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  # mechanism_frame — end-to-end mechanism cost over TCP from omq-compio bench
  build_mechanism_frame = lambda do
    mechanisms = %w[NULL CURVE BLAKE3ZMQ].select do |m|
      SIZE_LABELS.keys.any? { |s| latest.call('compio', 'mechanism', m, 1, s) }
    end
    sizes = SIZE_LABELS.keys.select do |s|
      mechanisms.any? { |m| latest.call('compio', 'mechanism', m, 1, s) }
    end
    return "\n(no mechanism data — run: cargo bench -p omq-compio --bench mechanism --features 'curve blake3zmq')\n" if mechanisms.empty?

    out = +"\n"
    out << "| Size | #{mechanisms.join(' | ')} |\n"
    out << "|---|#{mechanisms.map { '---:' }.join('|')}|\n"
    sizes.each do |sz|
      cells = mechanisms.map do |m|
        row = latest.call('compio', 'mechanism', m, 1, sz)
        row ? format_mbps_short(row[:mbps]) : '—'
      end
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  bm = File.read(BENCHMARKS_PATH)
  %w[compio tokio].each do |b|
    bm = replace_block.call(bm, "push_pull_1peer_#{b}",        b == 'compio' ? build_push_pull_1peer_compio.call : build_push_pull_1peer_tokio.call)
    bm = replace_block.call(bm, "push_pull_8peer_#{b}",        build_push_pull_8peer.call(b))
    bm = replace_block.call(bm, "req_rep_#{b}",                build_req_rep.call(b))
    bm = replace_block.call(bm, "pub_sub_#{b}",                build_pub_sub.call(b))
    bm = replace_block.call(bm, "router_dealer_#{b}",          build_router_dealer.call(b))
    bm = replace_block.call(bm, "pair_#{b}",                   build_pair.call(b))
    bm = replace_block.call(bm, "push_pull_priority_#{b}",     build_push_pull_priority.call(b))
  end
  bm = replace_block.call(bm, 'push_pull_fanout_8peer_tokio', build_push_pull_fanout_8peer.call('tokio'))
  bm = replace_block.call(bm, 'latency_percentiles',           build_latency_percentiles.call)
  bm = replace_block.call(bm, 'mechanism_frame',               build_mechanism_frame.call)
  File.write(BENCHMARKS_PATH, bm)

  run_counts = rows_by_backend.transform_values { |rows| rows.map { |r| r[:run_id] }.uniq.size }
  puts "Updated #{BENCHMARKS_PATH} (#{run_counts.map { |b, n| "#{b}: #{n} runs" }.join(', ')})"
  exit 0
end

# ── regression report ─────────────────────────────────────────────────────────

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

      fmt   = metric == :msgs_s ? method(:format_si) : method(:format_mbps_report)
      delta = ((new_val - old_val) / old_val.to_f * 100).round(1)
      row   = { pattern: pattern, transport: transport, peers: peer_label,
                size: size_label(msg_size), metric: metric,
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
    printf "\n  %-15s %-8s %-9s %-7s", pattern, transport, peer_label, size_label(msg_size)
    [:msgs_s, :mbps].each do |metric|
      fmt    = metric == :msgs_s ? method(:format_si) : method(:format_mbps_report)
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
