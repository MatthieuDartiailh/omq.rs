#!/usr/bin/env ruby
# frozen_string_literal: true

# Run every bench pattern across one or both backends, writing results to each
# crate's benches/results.jsonl with a shared OMQ_BENCH_RUN_ID.
#
# Usage:
#   ruby scripts/bench_run.rb                         # both backends, all patterns
#   ruby scripts/bench_run.rb --backend compio        # compio only
#   ruby scripts/bench_run.rb --backend tokio         # tokio only
#   ruby scripts/bench_run.rb --bench push_pull       # one bench target only
#   ruby scripts/bench_run.rb --features 'lz4 zstd'  # extra cargo features
#   ruby scripts/bench_run.rb --all-sizes             # full 7-size sweep
#   ruby scripts/bench_run.rb --id my-baseline        # named run ID
#
# Env knobs pass through to the bench harnesses:
#   OMQ_BENCH_SIZES=128,2048        payload sizes in bytes
#   OMQ_BENCH_TRANSPORTS=tcp,inproc subset of {inproc,ipc,tcp,...}
#   OMQ_BENCH_PEERS=1,3             peer counts
#   OMQ_BENCH_NO_WRITE=1            dry-run (suppress JSONL append)

require 'optparse'

ROOT = File.expand_path('..', __dir__)

options = { backends: %w[compio tokio] }

OptionParser.new do |o|
  o.banner = 'Usage: ruby scripts/bench_run.rb [options]'
  o.on('--backend BACKEND',  'Run only "compio" or "tokio"')         { |v| options[:backends]  = [v] }
  o.on('--bench TARGET',     'Run only this bench target (by name)') { |v| options[:bench]     = v }
  o.on('--features FEATS',   'Extra cargo --features value')               { |v| options[:features]       = v }
  o.on('--all-features',     'Enable lz4,zstd,curve,blake3zmq,ws') {
    options[:features] = 'lz4 zstd curve blake3zmq ws'
  }
  o.on('--all-sizes',        'Full 32 B–128 KiB size sweep (×4 steps, default: 128 B/2 KiB/8 KiB)') {
    options[:all_sizes] = true
  }
  o.on('--chart-sizes',      'Dense 8 B–256 KiB sweep (×2 steps, for charts)') {
    options[:chart_sizes] = true
  }
  o.on('--id RUN_ID',        'Override run ID (default: timestamp)')       { |v| options[:id]             = v }
end.parse!

run_id = options[:id] || Time.now.strftime('%Y-%m-%dT%H:%M:%SZ')
ENV['OMQ_BENCH_RUN_ID'] = run_id
# Use the env-var path so the flag isn't forwarded to libtest
# (which errors on unrecognized options when cargo bench runs lib unit tests).
ENV['OMQ_BENCH_SIZES'] = '32,128,512,2048,8192,32768,131072' if options[:all_sizes]
ENV['OMQ_BENCH_SIZES'] = '8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072,262144' if options[:chart_sizes]

puts "=== bench run #{run_id} ==="

# Split by transport: one process per transport avoids accumulating
# hundreds of TCP TIME_WAIT sockets in a single bench binary, which
# can stall connection handshakes late in a full sweep.
transport_groups = if ENV['OMQ_BENCH_TRANSPORTS']
                     [nil] # user already picked; pass through as-is
                   else
                     groups = %w[inproc ipc tcp]
                     groups << 'ws' if options[:features]&.include?('ws')
                     groups
                   end

unless options[:skip_main]
  options[:backends].each do |backend|
    crate = "omq-#{backend}"
    transport_groups.each do |transport|
      cmd = %w[cargo bench -p] + [crate]
      cmd += ['--features', options[:features]] if options[:features]
      cmd += ['--bench', options[:bench]] if options[:bench]

      env = {}
      env['OMQ_BENCH_TRANSPORTS'] = transport if transport
      label = transport ? "#{crate} [#{transport}]" : crate
      puts "\n--- #{label} ---"
      system(env, *cmd, chdir: ROOT) || abort("#{label} bench failed")
    end
  end
end

feats = options[:features] || ''
if feats.include?('curve') && feats.include?('blake3zmq')
  puts "\n--- mechanism (omq-compio, tcp) ---"
  cmd = %w[cargo bench -p omq-compio --bench mechanism --features] + ['curve blake3zmq']
  system(*cmd, chdir: ROOT) || abort('mechanism bench failed')
end

puts "\n=== done (#{run_id}) ==="
