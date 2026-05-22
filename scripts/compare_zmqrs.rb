#!/usr/bin/env ruby
# frozen_string_literal: true

# Compare zmq.rs (zeromq crate) vs omq-tokio vs omq-compio vs omq-zeromq:
# single PUSH -> single PULL, each cell: 3 s timed window after 500 ms warmup.
#
# zmq.rs is a pure-Rust async ZMQ implementation on tokio, making the
# omq-tokio comparison apples-to-apples. omq-compio runs on a single
# io_uring thread for contrast.
#
# By default runs ipc and tcp in order. zeromq 0.6 does not support inproc.
#
# IPC: omq peers use Linux abstract-namespace sockets (ipc://@name).
# zmq.rs does not support abstract namespaces and falls back to a socket
# file (/tmp/omq-bench-zmqrs-z-N.sock), which is cleaned up after each run.
#
# Usage:
#   ruby scripts/compare_zmqrs.rb                     # ipc + tcp
#   ruby scripts/compare_zmqrs.rb --ipc               # IPC only
#   ruby scripts/compare_zmqrs.rb --tcp               # TCP only
#   ruby scripts/compare_zmqrs.rb --latency           # also run latency
#   ruby scripts/compare_zmqrs.rb --update-benchmarks # update COMPARISONS.md
#   ruby scripts/compare_zmqrs.rb [port]              # override base TCP port

require 'optparse'
require 'fileutils'
require_relative 'lib/bench_compare'
require_relative 'lib/bench_helpers'

DURATION = 3
DEFAULT_BASE_PORT = 15_655
COMPARISONS_PATH = File.join(BenchCompare::ROOT, 'COMPARISONS.md')

# ---------- options ----------

options = { transports: nil, update_benchmarks: false, latency: false }
base_port = DEFAULT_BASE_PORT

OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/compare_zmqrs.rb [options] [port]'
  o.on('--ipc', 'IPC only') { options[:transports] = %w[ipc] }
  o.on('--tcp', 'TCP only') { options[:transports] = %w[tcp] }
  o.on('--latency', 'Also run latency comparison') { options[:latency] = true }
  o.on('--update-benchmarks', 'Update COMPARISONS.md') { options[:update_benchmarks] = true }
end.parse!

# Remaining positional arg: port override
base_port = ARGV.shift.to_i if ARGV.first&.match?(/\A\d+\z/)

transports = options[:transports] || %w[ipc tcp]

# ---------- cleanup ----------

at_exit do
  Dir.glob('/tmp/omq-bench-zmqrs-z-*.sock').each { |f| FileUtils.rm_f(f) }
end

# ---------- build ----------

zmqrs_dir = File.join(BenchCompare::ROOT, 'scripts', 'zmqrs_bench_peer')
zmqrs_peer = File.join(zmqrs_dir, 'target', 'release', 'zmqrs_bench_peer')

warn '==> building zmq.rs bench_peer...'
system('cargo', 'build', '--release', '-q', chdir: zmqrs_dir) ||
  abort('Failed to build zmqrs_bench_peer')

warn '==> building omq-tokio bench_peer...'
BenchCompare.cargo_build('omq-tokio', 'bench_peer_tokio')

warn '==> building omq-compio bench_peer...'
BenchCompare.cargo_build('omq-compio', 'bench_peer')

warn '==> building omq-zeromq bench_peer...'
BenchCompare.cargo_build('omq-zeromq', 'bench_peer_zeromq')

tokio_peer  = File.join(BenchCompare::ROOT, 'target', 'release', 'bench_peer_tokio')
compio_peer = File.join(BenchCompare::ROOT, 'target', 'release', 'bench_peer')
zeromq_peer = File.join(BenchCompare::ROOT, 'target', 'release', 'bench_peer_zeromq')

# ---------- versions ----------

zmqrs_version = BenchCompare.cargo_version(
  'zeromq', manifest: File.join(zmqrs_dir, 'Cargo.toml')
)
omq_version = BenchCompare.cargo_version('omq-tokio')

# ---------- address helpers ----------

def addr_for(transport, prefix, idx, base_port)
  case transport
  when 'tcp'
    offset = { 'z' => 0, 't' => 100, 'c' => 200, 'q' => 300 }.fetch(prefix)
    (base_port + offset + idx).to_s
  when 'ipc'
    if prefix == 'z'
      "ipc:///tmp/omq-bench-zmqrs-z-#{idx}.sock"
    else
      "ipc://@omq-bench-zmqrs-#{prefix}-#{idx}"
    end
  end
end

def cleanup_ipc(addr)
  return unless addr.start_with?('ipc:///tmp/')

  FileUtils.rm_f(addr.sub('ipc://', ''))
end

# ---------- throughput comparison ----------

def run_throughput_comparison(transport, base_port, zmqrs_peer, compio_peer,
                             tokio_peer, zeromq_peer, zmqrs_version,
                             omq_version, update_benchmarks)
  transport_label = case transport
                    when 'ipc' then 'IPC (zmq.rs: socket file; omq: abstract namespace)'
                    when 'tcp' then 'TCP'
                    end

  puts
  puts "zmq.rs (zeromq #{zmqrs_version}) vs omq #{omq_version}" \
       " -- #{transport_label}, #{DURATION}s window + 500ms warmup"
  puts
  printf "%-10s  %20s  %22s  %22s  %22s\n",
         '', 'zmq.rs', 'omq-compio', 'omq-tokio', 'omq-zeromq'
  printf "%-10s  %20s  %22s  %22s  %22s\n",
         'msg size', '(msg/s  |  MB/s)', '(msg/s  |  MB/s  | x)',
         '(msg/s  |  MB/s  | x)', '(msg/s  |  MB/s  | x)'
  puts '-' * 126

  results = []

  BenchCompare::COMPARISON_SIZES.each_with_index do |size, idx|
    addr_z = addr_for(transport, 'z', idx, base_port)
    addr_c = addr_for(transport, 'c', idx, base_port)
    addr_t = addr_for(transport, 't', idx, base_port)
    addr_q = addr_for(transport, 'q', idx, base_port)

    cleanup_ipc(addr_z)
    z = BenchCompare.run_throughput_cell(zmqrs_peer, transport, addr_z, size, DURATION)
    cleanup_ipc(addr_z)

    cleanup_ipc(addr_c)
    c = BenchCompare.run_throughput_cell(compio_peer, transport, addr_c, size, DURATION)
    cleanup_ipc(addr_c)

    cleanup_ipc(addr_t)
    t = BenchCompare.run_throughput_cell(tokio_peer, transport, addr_t, size, DURATION)
    cleanup_ipc(addr_t)

    cleanup_ipc(addr_q)
    q = BenchCompare.run_throughput_cell(zeromq_peer, transport, addr_q, size, DURATION)
    cleanup_ipc(addr_q)

    z_msgs = z&.[](:msgs_s)&.round || 0
    z_mb   = z&.[](:mbps)&.round(1) || 0
    c_msgs = c&.[](:msgs_s)&.round || 0
    c_mb   = c&.[](:mbps)&.round(1) || 0
    t_msgs = t&.[](:msgs_s)&.round || 0
    t_mb   = t&.[](:mbps)&.round(1) || 0
    q_msgs = q&.[](:msgs_s)&.round || 0
    q_mb   = q&.[](:mbps)&.round(1) || 0

    c_x = BenchHelpers.speedup_str(c_msgs, z_msgs)
    t_x = BenchHelpers.speedup_str(t_msgs, z_msgs)
    q_x = BenchHelpers.speedup_str(q_msgs, z_msgs)

    printf "  %7s    %9d msg/s  %6.1f MB/s    %9d msg/s  %6.1f MB/s  %6s" \
           "    %9d msg/s  %6.1f MB/s  %6s    %9d msg/s  %6.1f MB/s  %6s\n",
           BenchHelpers.size_label(size),
           z_msgs, z_mb,
           c_msgs, c_mb, c_x,
           t_msgs, t_mb, t_x,
           q_msgs, q_mb, q_x

    results << {
      size: size, z_msgs: z_msgs, z_mb: z_mb,
      c_msgs: c_msgs, c_mb: c_mb,
      t_msgs: t_msgs, t_mb: t_mb,
      q_msgs: q_msgs, q_mb: q_mb,
    }
  end

  puts

  return unless update_benchmarks

  update_throughput_tables(transport, results)
end

def update_throughput_tables(transport, results)
  marker = "zmqrs_comparison_#{transport}"
  md = File.read(COMPARISONS_PATH)

  # compio table
  compio_md = +""
  compio_md << "| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × |\n"
  compio_md << "|-------|-------------|------------|-----------------|----------------|---------|\n"
  results.each do |r|
    compio_md << "| #{BenchHelpers.size_label(r[:size])}" \
                 " | #{BenchHelpers.format_si(r[:z_msgs])}" \
                 " | #{BenchHelpers.format_mbps_bw(r[:z_mb])}" \
                 " | #{BenchHelpers.format_si(r[:c_msgs])}" \
                 " | #{BenchHelpers.format_mbps_bw(r[:c_mb])}" \
                 " | #{BenchHelpers.speedup_str(r[:c_msgs], r[:z_msgs])}" \
                 " |\n"
  end
  compio_md << "\n"
  md = BenchHelpers.replace_block(md, "#{marker}_compio", compio_md)

  # tokio table (includes omq-zeromq columns)
  tokio_md = +""
  tokio_md << "| Size | zmq.rs msg/s | zmq.rs MB/s | omq-tokio msg/s | omq-tokio MB/s" \
              " | tokio × | omq-zeromq msg/s | omq-zeromq MB/s | zeromq × |\n"
  tokio_md << "|-------|-------------|------------|----------------|---------------" \
              "|---------|-----------------|----------------|---------|\n"
  results.each do |r|
    tokio_md << "| #{BenchHelpers.size_label(r[:size])}" \
                " | #{BenchHelpers.format_si(r[:z_msgs])}" \
                " | #{BenchHelpers.format_mbps_bw(r[:z_mb])}" \
                " | #{BenchHelpers.format_si(r[:t_msgs])}" \
                " | #{BenchHelpers.format_mbps_bw(r[:t_mb])}" \
                " | #{BenchHelpers.speedup_str(r[:t_msgs], r[:z_msgs])}" \
                " | #{BenchHelpers.format_si(r[:q_msgs])}" \
                " | #{BenchHelpers.format_mbps_bw(r[:q_mb])}" \
                " | #{BenchHelpers.speedup_str(r[:q_msgs], r[:z_msgs])}" \
                " |\n"
  end
  tokio_md << "\n"
  md = BenchHelpers.replace_block(md, "#{marker}_tokio", tokio_md)

  File.write(COMPARISONS_PATH, md)
  warn "Updated #{COMPARISONS_PATH} (#{marker}_compio, #{marker}_tokio)"
end

# ---------- latency comparison ----------

def run_latency_comparison(transport, base_port, zmqrs_peer, compio_peer,
                           tokio_peer, zmqrs_version, omq_version,
                           update_benchmarks)
  transport_label = case transport
                    when 'ipc' then 'IPC (zmq.rs: socket file; omq: abstract namespace)'
                    when 'tcp' then 'TCP'
                    end

  puts
  puts "Latency: zmq.rs (zeromq #{zmqrs_version}) vs omq #{omq_version}" \
       " -- #{transport_label}, REQ/REP round-trip"
  puts
  printf "%-10s  %12s  %12s  %16s  %16s  %10s  %15s  %15s  %10s\n",
         'msg size', 'zmq.rs p50', 'zmq.rs p99',
         'omq-compio p50', 'omq-compio p99', 'compio x',
         'omq-tokio p50', 'omq-tokio p99', 'tokio x'
  puts '-' * 135

  results = []

  BenchCompare::LATENCY_SIZES.each_with_index do |size, idx|
    addr_z = addr_for(transport, 'z', idx, base_port)
    addr_c = addr_for(transport, 'c', idx, base_port)
    addr_t = addr_for(transport, 't', idx, base_port)

    cleanup_ipc(addr_z)
    z = BenchCompare.run_latency_cell(zmqrs_peer, transport, addr_z, size)
    cleanup_ipc(addr_z)

    cleanup_ipc(addr_c)
    c = BenchCompare.run_latency_cell(compio_peer, transport, addr_c, size)
    cleanup_ipc(addr_c)

    cleanup_ipc(addr_t)
    t = BenchCompare.run_latency_cell(tokio_peer, transport, addr_t, size)
    cleanup_ipc(addr_t)

    c_x = BenchHelpers.latency_speedup_str(z[:p50_us], c[:p50_us])
    t_x = BenchHelpers.latency_speedup_str(z[:p50_us], t[:p50_us])

    printf "  %7s    %10s  %10s    %14s  %14s  %10s    %13s  %13s  %10s\n",
           BenchHelpers.size_label(size),
           BenchHelpers.format_us(z[:p50_us]), BenchHelpers.format_us(z[:p99_us]),
           BenchHelpers.format_us(c[:p50_us]), BenchHelpers.format_us(c[:p99_us]), c_x,
           BenchHelpers.format_us(t[:p50_us]), BenchHelpers.format_us(t[:p99_us]), t_x

    results << {
      size: size,
      z_p50: z[:p50_us], z_p99: z[:p99_us],
      c_p50: c[:p50_us], c_p99: c[:p99_us],
      t_p50: t[:p50_us], t_p99: t[:p99_us],
    }
  end

  puts

  return unless update_benchmarks

  update_latency_table(transport, results)
end

def update_latency_table(transport, results)
  marker = "zmqrs_latency_#{transport}"
  md = File.read(COMPARISONS_PATH)

  table_md = +""
  table_md << "| Size | zmq.rs p50 | zmq.rs p99 | omq-compio p50 | omq-compio p99" \
              " | compio × | omq-tokio p50 | omq-tokio p99 | tokio × |\n"
  table_md << "|-------|-----------|-----------|---------------|---------------" \
              "|---------|--------------|--------------|--------|\n"
  results.each do |r|
    table_md << "| #{BenchHelpers.size_label(r[:size])}" \
                " | #{BenchHelpers.format_us(r[:z_p50])}" \
                " | #{BenchHelpers.format_us(r[:z_p99])}" \
                " | #{BenchHelpers.format_us(r[:c_p50])}" \
                " | #{BenchHelpers.format_us(r[:c_p99])}" \
                " | #{BenchHelpers.latency_speedup_str(r[:z_p50], r[:c_p50])}" \
                " | #{BenchHelpers.format_us(r[:t_p50])}" \
                " | #{BenchHelpers.format_us(r[:t_p99])}" \
                " | #{BenchHelpers.latency_speedup_str(r[:z_p50], r[:t_p50])}" \
                " |\n"
  end
  table_md << "\n"

  md = BenchHelpers.replace_block(md, marker, table_md)
  File.write(COMPARISONS_PATH, md)
  warn "Updated #{COMPARISONS_PATH} (#{marker})"
end

# ---------- main ----------

transports.each do |transport|
  run_throughput_comparison(
    transport, base_port, zmqrs_peer, compio_peer, tokio_peer, zeromq_peer,
    zmqrs_version, omq_version, options[:update_benchmarks]
  )
end

if options[:latency]
  # omq-zeromq bench_peer does not have req/rep mode, so excluded from latency
  transports.each do |transport|
    run_latency_comparison(
      transport, base_port, zmqrs_peer, compio_peer, tokio_peer,
      zmqrs_version, omq_version, options[:update_benchmarks]
    )
  end
end
