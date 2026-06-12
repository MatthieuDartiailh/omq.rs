#!/usr/bin/env ruby
# frozen_string_literal: true

# Update BENCHMARKS_COMPRESSION.md tables from compression bench JSONL results.
#
# The bench runs at full loopback speed.  This script projects throughput
# at the requested link speed:
#   effective_msgs_s = min(cpu_msgs_s, link_bytes_s / wire_bytes)
#
#   cargo bench -p omq-compio --features lz4 --bench compression
#   cargo bench -p omq-tokio  --features lz4 --bench compression
#   ruby scripts/compression_report.rb --link 100m
#   ruby scripts/compression_report.rb --link 100m --backend tokio

require 'optparse'
require_relative 'lib/bench_helpers'

ROOT = File.expand_path('..', __dir__)
COMPRESSION_PATH = File.join(ROOT, 'BENCHMARKS_COMPRESSION.md')
DEFAULT_BACKEND = 'compio'

LINK_BYTES_S = {
  '1g'   => 1_000_000_000.0 / 8,
  '100m' => 100_000_000.0 / 8,
  '10m'  => 10_000_000.0 / 8,
}.freeze

options = { link: nil, prefix: nil, backend: DEFAULT_BACKEND }
OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/compression_report.rb --link 10m|100m|1g [--backend compio|tokio]'
  o.on('--link LINK', '10m, 100m, or 1g: which section to update') { |v| options[:link] = v }
  o.on('--backend BACKEND', 'compio or tokio (default: compio)') { |v| options[:backend] = v }
  o.on('--run-prefix PREFIX', 'Select rows by run ID prefix') { |v| options[:prefix] = v }
end.parse!

abort 'Error: --link is required (10m, 100m, or 1g)' unless options[:link]
abort "Error: --link must be one of: #{LINK_BYTES_S.keys.join(', ')}" unless LINK_BYTES_S.key?(options[:link])

jsonl_path = File.join(ROOT, "omq-#{options[:backend]}", 'benches', 'results_compression.jsonl')
rows = BenchHelpers.load_jsonl(jsonl_path).select do |r|
  %i[compression_json compression_json_dict].include?(r[:pattern].to_sym)
end

abort 'No compression_json rows in results_compression.jsonl' if rows.empty?

rows.sort_by! { |r| r[:run_id] }

if options[:prefix]
  selected = rows.select { |r| r[:run_id].start_with?(options[:prefix]) }
else
  latest_id = rows.last[:run_id]
  selected = rows.select { |r| r[:run_id] == latest_id }
end

abort 'No rows matched' if selected.empty?
warn "Using #{selected.size} rows from #{selected.last[:run_id]}"

link_bytes_s = LINK_BYTES_S[options[:link]]

data = {}
selected.each do |r|
  key = [r[:pattern].to_sym, r[:transport], r[:msg_size]]
  cpu_msgs_s = r[:msgs_s]
  wire_bytes = r[:wire_bytes] || r[:msg_size]
  wire_limited = link_bytes_s / [wire_bytes, 1].max
  eff_msgs_s = [cpu_msgs_s, wire_limited].min
  eff_mbps = eff_msgs_s * r[:msg_size] / 1_000_000.0
  data[key] = r.merge(msgs_s: eff_msgs_s, mbps: eff_mbps)
end

def build_table(data, pattern)
  transports = %w[tcp lz4+tcp]
  transports = %w[lz4+tcp] if pattern == :compression_json_dict
  sizes = data.keys
    .select { |p, _t, _s| p == pattern }
    .map { |_p, _t, s| s }
    .uniq.sort
    .select { |s| BenchHelpers::TABLE_SIZES.include?(s) }

  return "(no data)\n" if sizes.empty?

  out = +""
  out << "| Size | #{transports.map { |t| "#{t} msg/s" }.join(' | ')} | #{transports.map { |t| "#{t} virt" }.join(' | ')} |\n"
  out << "|---|#{'---:|' * transports.size}#{'---:|' * transports.size}\n"
  sizes.each do |sz|
    msgs_cells = transports.map do |t|
      r = data[[pattern, t, sz]]
      r ? BenchHelpers.format_si(r[:msgs_s], nil_str: '---') : '---'
    end
    tput_cells = transports.map do |t|
      r = data[[pattern, t, sz]]
      r ? BenchHelpers.format_mbps(r[:mbps], nil_str: '---') : '---'
    end
    out << "| #{BenchHelpers.size_label(sz)} | #{msgs_cells.join(' | ')} | #{tput_cells.join(' | ')} |\n"
  end
  out << "\n"
  out
end

link = options[:link]
md = File.read(COMPRESSION_PATH)
md = BenchHelpers.replace_block(md, "compression_#{link}",      build_table(data, :compression_json))
md = BenchHelpers.replace_block(md, "compression_#{link}_dict", build_table(data, :compression_json_dict))
File.write(COMPRESSION_PATH, md)
warn "Updated #{COMPRESSION_PATH} (#{link} sections)"
