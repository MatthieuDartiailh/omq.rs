#!/usr/bin/env ruby
# frozen_string_literal: true

# Update COMPRESSION.md tables from compression bench JSONL results.
#
# Each invocation updates one link-speed section. Run the bench at each
# speed, then update the corresponding section:
#
#   sudo tc qdisc replace dev lo root tbf rate 100mbit burst 128kb latency 1ms
#   cargo bench -p omq-compio --features lz4,zstd --bench compression
#   ruby scripts/compression_report.rb --link 100m
#   sudo tc qdisc del dev lo root
#
#   sudo tc qdisc replace dev lo root tbf rate 1gbit burst 128kb latency 1ms
#   cargo bench -p omq-compio --features lz4,zstd --bench compression
#   ruby scripts/compression_report.rb --link 1g
#   sudo tc qdisc del dev lo root

require 'optparse'
require_relative 'lib/bench_helpers'

ROOT = File.expand_path('..', __dir__)
COMPRESSION_PATH = File.join(ROOT, 'BENCHMARKS_COMPRESSION.md')
JSONL_PATH = File.join(ROOT, 'omq-compio', 'benches', 'results.jsonl')

options = { link: nil, prefix: nil }
OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/compression_report.rb --link 100m|1g [--run-prefix PREFIX]'
  o.on('--link LINK', '100m or 1g: which section to update') { |v| options[:link] = v }
  o.on('--run-prefix PREFIX', 'Select rows by run ID prefix') { |v| options[:prefix] = v }
end.parse!

abort 'Error: --link is required (100m or 1g)' unless options[:link]
abort 'Error: --link must be 100m or 1g' unless %w[100m 1g].include?(options[:link])

rows = BenchHelpers.load_jsonl(JSONL_PATH).select do |r|
  %i[compression_json compression_json_dict].include?(r[:pattern].to_sym)
end

abort 'No compression_json rows in results.jsonl' if rows.empty?

rows.sort_by! { |r| r[:run_id] }

if options[:prefix]
  selected = rows.select { |r| r[:run_id].start_with?(options[:prefix]) }
else
  latest_id = rows.last[:run_id]
  latest_ts = latest_id[/ts-(\d+)/, 1]&.to_i || 0
  selected = rows.select do |r|
    ts = r[:run_id][/ts-(\d+)/, 1]&.to_i || 0
    (ts - latest_ts).abs < 600
  end
  selected = rows.last(30) if selected.empty?
end

warn "Using #{selected.size} rows near #{selected.last[:run_id]}"

data = {}
selected.each do |r|
  key = [r[:pattern].to_sym, r[:transport], r[:msg_size]]
  data[key] = r
end

def build_table(data, pattern)
  transports = %w[tcp lz4+tcp zstd+tcp]
  transports = %w[lz4+tcp zstd+tcp] if pattern == :compression_json_dict
  sizes = data.keys
    .select { |p, _t, _s| p == pattern }
    .map { |_p, _t, s| s }
    .uniq.sort

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
