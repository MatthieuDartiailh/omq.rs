#!/usr/bin/env ruby
# frozen_string_literal: true

# Compare omq-compio + omq-tokio vs libzmq: single PUSH process -> single PULL
# process. Each cell: 3 s timed window after 500 ms warmup.
#
# By default runs inproc, ipc, and tcp in order. Pass a transport flag to
# limit to one transport.
#
# IPC uses Linux abstract-namespace sockets (ipc://@name); no socket files
# are created.
#
# Inproc requires both sockets in the same process, so each peer binary
# runs its own push+pull internally (bench_peer inproc / libzmq_bench_peer
# inproc).
#
# Usage:
#   ./scripts/compare_libzmq.rb                          # all transports
#   ./scripts/compare_libzmq.rb --inproc                 # inproc only
#   ./scripts/compare_libzmq.rb --ipc                    # IPC only
#   ./scripts/compare_libzmq.rb --tcp                    # TCP only
#   ./scripts/compare_libzmq.rb --latency                # also run latency
#   ./scripts/compare_libzmq.rb --update-benchmarks      # update COMPARISONS.md
#   ./scripts/compare_libzmq.rb [port]                   # override base TCP port

require 'optparse'
require_relative 'lib/bench_compare'
require_relative 'lib/bench_helpers'

DURATION = 3
COMPARISONS_PATH = File.join(BenchCompare::ROOT, 'COMPARISONS.md')

base_port = 15_555
update_benchmarks = false
run_latency = false
chart_sizes = false
transport_filter = nil

OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options] [port]"
  opts.on('--inproc',            'inproc only')           { transport_filter = 'inproc' }
  opts.on('--ipc',               'IPC only')              { transport_filter = 'ipc' }
  opts.on('--tcp',               'TCP only')              { transport_filter = 'tcp' }
  opts.on('--ws',                'WebSocket only')        { transport_filter = 'ws' }
  opts.on('--chart-sizes',       'dense ×2 step sweep (8 B – 256 KiB)') { chart_sizes = true }
  opts.on('--latency',           'also run latency comparison') { run_latency = true }
  opts.on('--update-benchmarks', 'update COMPARISONS.md') { update_benchmarks = true }
end.parse!

SIZES = chart_sizes ? BenchCompare::CHART_COMPARISON_SIZES : BenchCompare::COMPARISON_SIZES
LATENCY_SIZES = chart_sizes ? BenchCompare::CHART_LATENCY_SIZES : BenchCompare::LATENCY_SIZES

# Remaining positional arg is base port.
base_port = ARGV.shift.to_i if ARGV.first&.match?(/\A\d+\z/)

transports = transport_filter ? [transport_filter] : %w[inproc ipc tcp ws]

# ---------- build ----------

ws_needed = transports.include?('ws')
ws_features = ws_needed ? ['ws'] : []

$stderr.puts '==> building omq-compio bench_peer...'
BenchCompare.cargo_build('omq-compio', 'bench_peer', features: ws_features)
omq_peer = File.join(BenchCompare::ROOT, 'target/release/bench_peer')

$stderr.puts '==> building omq-tokio bench_peer...'
BenchCompare.cargo_build('omq-tokio', 'bench_peer_tokio', features: ws_features)
tokio_peer = File.join(BenchCompare::ROOT, 'target/release/bench_peer_tokio')

$stderr.puts '==> building libzmq bench_peer...'
script_dir = File.expand_path(__dir__)
libzmq_peer = File.join(script_dir, 'libzmq_bench_peer')
BenchCompare.gcc_build(File.join(script_dir, 'libzmq_bench_peer.c'), libzmq_peer)

# ---------- versions ----------

omq_version = BenchCompare.cargo_version('omq-compio')
zmq_version = `pkg-config --modversion libzmq 2>/dev/null`.strip
zmq_version = '?' if zmq_version.empty?

# ---------- helpers ----------

def addr_for(transport, prefix, idx, base_port)
  case transport
  when 'tcp'
    offset = { 'o' => 0, 't' => 100, 'z' => 200 }.fetch(prefix, 0)
    (base_port + offset + idx).to_s
  when 'ws'
    offset = { 'o' => 300, 't' => 400, 'z' => 500 }.fetch(prefix, 300)
    "ws://127.0.0.1:#{base_port + offset + idx}/"
  when 'ipc'
    "ipc://@omq-bench-lzq-#{prefix}-#{idx}"
  when 'inproc'
    "bench-lzq-#{idx}"
  end
end

# ---------- throughput ----------

def run_comparison(transport, peers, base_port, update_benchmarks, omq_version, zmq_version)
  omq_peer, tokio_peer, libzmq_peer = peers

  transport_label = case transport
                    when 'inproc' then 'inproc (same process)'
                    when 'ipc'    then 'IPC (abstract namespace)'
                    when 'tcp'    then 'TCP'
                    when 'ws'     then 'WebSocket'
                    end

  puts
  puts "omq #{omq_version} vs libzmq #{zmq_version} — #{transport_label}, #{DURATION}s window + 500ms warmup"
  puts

  if transport == 'inproc'
    printf "%-10s  %20s  %22s  %22s  %22s\n",
           '', 'libzmq', 'omq-compio (mt)', 'omq-compio (st)', 'omq-tokio'
    printf "%-10s  %20s  %22s  %22s  %22s\n",
           'msg size', '(msg/s  |  MB/s)', '(msg/s  |  MB/s  | x)',
           '(msg/s  |  MB/s  | x)', '(msg/s  |  MB/s  | x)'
    puts '-' * 126
  else
    printf "%-10s  %20s  %22s  %22s\n",
           '', 'libzmq', 'omq-compio', 'omq-tokio'
    printf "%-10s  %20s  %22s  %22s\n",
           'msg size', '(msg/s  |  MB/s)', '(msg/s  |  MB/s  | x)',
           '(msg/s  |  MB/s  | x)'
    puts '-' * 107
  end

  results = []

  SIZES.each_with_index do |size, idx|
    addr_o = addr_for(transport, 'o', idx, base_port)
    addr_t = addr_for(transport, 't', idx, base_port)
    addr_z = addr_for(transport, 'z', idx, base_port)

    omq   = BenchCompare.run_throughput_cell(omq_peer, transport, addr_o, size, DURATION)
    tokio = BenchCompare.run_throughput_cell(tokio_peer, transport, addr_t, size, DURATION)
    lzq   = BenchCompare.run_throughput_cell(libzmq_peer, transport, addr_z, size, DURATION)

    omq_ratio   = BenchHelpers.speedup_str(omq&.[](:msgs_s), lzq&.[](:msgs_s))
    tokio_ratio = BenchHelpers.speedup_str(tokio&.[](:msgs_s), lzq&.[](:msgs_s))

    row = {
      size: size,
      omq: omq, tokio: tokio, lzq: lzq,
      omq_ratio: omq_ratio, tokio_ratio: tokio_ratio,
    }

    if transport == 'inproc'
      addr_st = "#{addr_o}-st"
      omq_st = BenchCompare.run_inproc_cell(omq_peer, addr_st, size, DURATION, subcmd: 'inproc-st')
      omq_st_ratio = BenchHelpers.speedup_str(omq_st&.[](:msgs_s), lzq&.[](:msgs_s))
      row[:omq_st] = omq_st
      row[:omq_st_ratio] = omq_st_ratio

      printf "  %7s    %9.0f msg/s  %6.1f MB/s    %9.0f msg/s  %6.1f MB/s  %6s    %9.0f msg/s  %6.1f MB/s  %6s    %9.0f msg/s  %6.1f MB/s  %6s\n",
             BenchHelpers.size_label(size),
             lzq[:msgs_s], lzq[:mbps],
             omq[:msgs_s], omq[:mbps], omq_ratio,
             omq_st[:msgs_s], omq_st[:mbps], omq_st_ratio,
             tokio[:msgs_s], tokio[:mbps], tokio_ratio
    else
      printf "  %7s    %9.0f msg/s  %6.1f MB/s    %9.0f msg/s  %6.1f MB/s  %6s    %9.0f msg/s  %6.1f MB/s  %6s\n",
             BenchHelpers.size_label(size),
             lzq[:msgs_s], lzq[:mbps],
             omq[:msgs_s], omq[:mbps], omq_ratio,
             tokio[:msgs_s], tokio[:mbps], tokio_ratio
    end

    results << row
  end

  puts

  return unless update_benchmarks

  marker = "libzmq_comparison_#{transport}"
  text = File.read(COMPARISONS_PATH)

  # -- compio table --
  compio_md = +''
  if transport == 'inproc'
    compio_md << "| Size | libzmq msg/s | libzmq MB/s | compio-mt msg/s | compio-mt MB/s | mt × | compio-st msg/s | compio-st MB/s | st × |\n"
    compio_md << "|-------|-------------|------------|----------------|---------------|------|----------------|---------------|------|\n"
  else
    compio_md << "| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × |\n"
    compio_md << "|-------|-------------|------------|-----------------|----------------|----------|\n"
  end

  results.each do |r|
    zmq_fmt   = BenchHelpers.format_si(r[:lzq][:msgs_s])
    zmq_bw    = BenchHelpers.format_mbps_bw(r[:lzq][:mbps])
    omq_fmt   = BenchHelpers.format_si(r[:omq][:msgs_s])
    omq_bw    = BenchHelpers.format_mbps_bw(r[:omq][:mbps])
    label     = BenchHelpers.size_label(r[:size])

    if transport == 'inproc'
      omq_st_fmt = BenchHelpers.format_si(r[:omq_st][:msgs_s])
      omq_st_bw  = BenchHelpers.format_mbps_bw(r[:omq_st][:mbps])
      compio_md << "| #{label} | #{zmq_fmt} | #{zmq_bw} | #{omq_fmt} | #{omq_bw} | #{r[:omq_ratio]} | #{omq_st_fmt} | #{omq_st_bw} | #{r[:omq_st_ratio]} |\n"
    else
      compio_md << "| #{label} | #{zmq_fmt} | #{zmq_bw} | #{omq_fmt} | #{omq_bw} | #{r[:omq_ratio]} |\n"
    end
  end
  compio_md << "\n"

  # -- tokio table --
  tokio_md = +''
  if transport == 'inproc'
    tokio_md << "| Size | libzmq msg/s | libzmq MB/s | tokio msg/s | tokio MB/s | tokio × |\n"
    tokio_md << "|-------|-------------|------------|------------|-----------|----------|\n"
  else
    tokio_md << "| Size | libzmq msg/s | libzmq MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |\n"
    tokio_md << "|-------|-------------|------------|----------------|---------------|----------|\n"
  end

  results.each do |r|
    zmq_fmt   = BenchHelpers.format_si(r[:lzq][:msgs_s])
    zmq_bw    = BenchHelpers.format_mbps_bw(r[:lzq][:mbps])
    tokio_fmt = BenchHelpers.format_si(r[:tokio][:msgs_s])
    tokio_bw  = BenchHelpers.format_mbps_bw(r[:tokio][:mbps])
    label     = BenchHelpers.size_label(r[:size])

    tokio_md << "| #{label} | #{zmq_fmt} | #{zmq_bw} | #{tokio_fmt} | #{tokio_bw} | #{r[:tokio_ratio]} |\n"
  end
  tokio_md << "\n"

  text = BenchHelpers.replace_block(text, "#{marker}_compio", compio_md)
  text = BenchHelpers.replace_block(text, "#{marker}_tokio", tokio_md)
  File.write(COMPARISONS_PATH, text)
  $stderr.puts "Updated #{COMPARISONS_PATH}"
end

# ---------- latency ----------

def run_latency_comparison(transport, peers, base_port, update_benchmarks, omq_version, zmq_version)
  omq_peer, tokio_peer, libzmq_peer = peers

  transport_label = case transport
                    when 'ipc' then 'IPC (abstract namespace)'
                    when 'tcp' then 'TCP'
                    when 'ws'  then 'WebSocket'
                    end

  puts
  puts "Latency: omq #{omq_version} vs libzmq #{zmq_version} — #{transport_label}, REQ/REP round-trip"
  puts

  printf "%-10s  %12s %12s  %14s %14s %10s  %13s %13s %10s\n",
         '', 'libzmq p50', 'libzmq p99',
         'omq-compio p50', 'omq-compio p99', 'compio ×',
         'omq-tokio p50', 'omq-tokio p99', 'tokio ×'
  puts '-' * 118

  results = []

  LATENCY_SIZES.each_with_index do |size, idx|
    addr_o = addr_for(transport, 'o', idx, base_port)
    addr_t = addr_for(transport, 't', idx, base_port)
    addr_z = addr_for(transport, 'z', idx, base_port)

    lzq   = BenchCompare.run_latency_cell(libzmq_peer, transport, addr_z, size)
    omq   = BenchCompare.run_latency_cell(omq_peer, transport, addr_o, size)
    tokio = BenchCompare.run_latency_cell(tokio_peer, transport, addr_t, size)

    compio_speedup = BenchHelpers.latency_speedup_str(lzq[:p50_us], omq[:p50_us])
    tokio_speedup  = BenchHelpers.latency_speedup_str(lzq[:p50_us], tokio[:p50_us])

    printf "  %7s    %12s %12s  %14s %14s %10s  %13s %13s %10s\n",
           BenchHelpers.size_label(size),
           BenchHelpers.format_us(lzq[:p50_us]), BenchHelpers.format_us(lzq[:p99_us]),
           BenchHelpers.format_us(omq[:p50_us]), BenchHelpers.format_us(omq[:p99_us]), compio_speedup,
           BenchHelpers.format_us(tokio[:p50_us]), BenchHelpers.format_us(tokio[:p99_us]), tokio_speedup

    results << {
      size: size, lzq: lzq, omq: omq, tokio: tokio,
      compio_speedup: compio_speedup, tokio_speedup: tokio_speedup,
    }
  end

  puts

  return unless update_benchmarks

  marker = "libzmq_latency_#{transport}"
  text = File.read(COMPARISONS_PATH)

  md = +''
  md << "| Size | libzmq p50 | libzmq p99 | omq-compio p50 | omq-compio p99 | compio × | omq-tokio p50 | omq-tokio p99 | tokio × |\n"
  md << "|-------|-----------|-----------|---------------|---------------|---------|--------------|--------------|--------|\n"

  results.each do |r|
    label = BenchHelpers.size_label(r[:size])
    md << "| #{label} | #{BenchHelpers.format_us(r[:lzq][:p50_us])} | #{BenchHelpers.format_us(r[:lzq][:p99_us])}" \
         " | #{BenchHelpers.format_us(r[:omq][:p50_us])} | #{BenchHelpers.format_us(r[:omq][:p99_us])}" \
         " | #{r[:compio_speedup]}" \
         " | #{BenchHelpers.format_us(r[:tokio][:p50_us])} | #{BenchHelpers.format_us(r[:tokio][:p99_us])}" \
         " | #{r[:tokio_speedup]} |\n"
  end
  md << "\n"

  text = BenchHelpers.replace_block(text, marker, md)
  File.write(COMPARISONS_PATH, text)
  $stderr.puts "Updated #{COMPARISONS_PATH} (#{marker})"
end

# ---------- run ----------

peers = [omq_peer, tokio_peer, libzmq_peer]

transports.each do |transport|
  run_comparison(transport, peers, base_port, update_benchmarks, omq_version, zmq_version)
end

if run_latency
  latency_transports = transports - ['inproc']
  latency_transports.each do |transport|
    run_latency_comparison(transport, peers, base_port, update_benchmarks, omq_version, zmq_version)
  end
end
