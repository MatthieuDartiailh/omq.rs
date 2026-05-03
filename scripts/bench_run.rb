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
  o.on('--all-features',     'Enable lz4,zstd,curve,blake3zmq (not priority; use --with-priority)') {
    options[:features] = 'lz4 zstd curve blake3zmq'
  }
  o.on('--all-sizes',        'Full 32 B–128 KiB size sweep (default: 128 B/2 KiB/32 KiB)') {
    options[:all_sizes] = true
  }
  o.on('--with-priority',    'Also run push_pull with priority (→ results_priority.jsonl)') {
    options[:with_priority] = true
  }
  o.on('--id RUN_ID',        'Override run ID (default: timestamp)')       { |v| options[:id]             = v }
end.parse!

run_id = options[:id] || Time.now.strftime('%Y-%m-%dT%H:%M:%SZ')
ENV['OMQ_BENCH_RUN_ID'] = run_id

puts "=== bench run #{run_id} ==="

options[:backends].each do |backend|
  crate = "omq-#{backend}"
  cmd   = %w[cargo bench -p] + [crate]
  cmd  += ['--features', options[:features]] if options[:features]
  cmd  += ['--bench',    options[:bench]]    if options[:bench]
  cmd  += ['--', '--all-sizes']              if options[:all_sizes]

  puts "\n--- #{crate} ---"
  system(*cmd, chdir: ROOT) || abort("#{crate} bench failed")
end

if options[:with_priority]
  ENV['OMQ_BENCH_RESULTS_SUFFIX'] = 'priority'
  options[:backends].each do |backend|
    crate = "omq-#{backend}"
    feat  = [options[:features], 'priority'].compact.join(' ')
    cmd   = %w[cargo bench -p] + [crate, '--features', feat, '--bench', 'push_pull']
    cmd  += ['--', '--all-sizes'] if options[:all_sizes]
    puts "\n--- #{crate} (priority) ---"
    system(*cmd, chdir: ROOT) || abort("#{crate} priority bench failed")
  end
  ENV.delete('OMQ_BENCH_RESULTS_SUFFIX')
end

puts "\n=== done (#{run_id}) ==="
