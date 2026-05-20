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

require 'json'
require 'optparse'

ROOT = File.expand_path('..', __dir__)
COMPRESSION_PATH = File.join(ROOT, 'BENCHMARKS_COMPRESSION.md')
JSONL_PATH = File.join(ROOT, 'omq-compio', 'benches', 'results.jsonl')

SIZE_LABELS = {
  32 => '32 B', 64 => '64 B', 128 => '128 B', 256 => '256 B',
  512 => '512 B', 1_024 => '1 KiB', 2_048 => '2 KiB', 4_096 => '4 KiB',
  8_192 => '8 KiB', 32_768 => '32 KiB', 131_072 => '128 KiB',
}.freeze

def size_label(n) = SIZE_LABELS[n] || "#{n} B"

def format_tput(mbps)
  return '---' unless mbps && mbps > 0
  if    mbps >= 1_000 then '%.1f GB/s' % (mbps / 1_000.0)
  elsif mbps >= 100   then '%.0f MB/s' % mbps
  elsif mbps >= 10    then '%.1f MB/s' % mbps
  else                     '%.2f MB/s' % mbps
  end
end

def format_si(v)
  return '---' unless v && v > 0
  if    v >= 1e6   then '%.2fM' % (v / 1e6)
  elsif v >= 100e3 then '%.0fk' % (v / 1e3)
  elsif v >= 1e3   then '%.1fk' % (v / 1e3)
  else                  '%.0f'  % v
  end
end

options = { link: nil, prefix: nil }
OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/compression_report.rb --link 100m|1g [--run-prefix PREFIX]'
  o.on('--link LINK', '100m or 1g: which section to update') { |v| options[:link] = v }
  o.on('--run-prefix PREFIX', 'Select rows by run ID prefix') { |v| options[:prefix] = v }
end.parse!

abort 'Error: --link is required (100m or 1g)' unless options[:link]
abort 'Error: --link must be 100m or 1g' unless %w[100m 1g].include?(options[:link])

rows = File.readlines(JSONL_PATH).filter_map do |line|
  line = line.strip
  next if line.empty?
  r = JSON.parse(line, symbolize_names: true)
  next unless %i[compression_json compression_json_dict].include?(r[:pattern].to_sym)
  r
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
      r ? format_si(r[:msgs_s]) : '---'
    end
    tput_cells = transports.map do |t|
      r = data[[pattern, t, sz]]
      r ? format_tput(r[:mbps]) : '---'
    end
    out << "| #{size_label(sz)} | #{msgs_cells.join(' | ')} | #{tput_cells.join(' | ')} |\n"
  end
  out << "\n"
  out
end

def replace_block(text, marker, content)
  re = /<!-- BEGIN #{Regexp.escape(marker)} -->\n.*?<!-- END #{Regexp.escape(marker)} -->/m
  replacement = "<!-- BEGIN #{marker} -->\n#{content}<!-- END #{marker} -->"
  text.sub(re, replacement)
end

link = options[:link]
md = File.read(COMPRESSION_PATH)
md = replace_block(md, "compression_#{link}",      build_table(data, :compression_json))
md = replace_block(md, "compression_#{link}_dict",  build_table(data, :compression_json_dict))
File.write(COMPRESSION_PATH, md)
warn "Updated #{COMPRESSION_PATH} (#{link} sections)"
