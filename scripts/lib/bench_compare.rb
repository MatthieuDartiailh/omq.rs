# frozen_string_literal: true

require_relative 'bench_helpers'

module BenchCompare
  ROOT = File.expand_path('../..', __dir__)

  module_function

  def cargo_build(crate, bin, quiet: true, features: [])
    cmd = ['cargo', 'build', '--release', '-p', crate, '--bin', bin]
    cmd += ['--features', features.join(',')] unless features.empty?
    cmd << '-q' if quiet
    system(*cmd, chdir: ROOT) || abort("Failed to build #{crate}/#{bin}")
  end

  def gcc_build(src, out, libs: %w[-lzmq -lpthread])
    system('gcc', '-O2', '-o', out, src, *libs) || abort("Failed to build #{src}")
  end

  def cargo_version(crate, manifest: nil)
    args = ['cargo', 'metadata', '--format-version', '1']
    args += ['--no-deps'] unless manifest
    args += ['--manifest-path', manifest] if manifest
    json = `#{args.join(' ')} 2>/dev/null`
    pkgs = JSON.parse(json)['packages']
    pkgs.find { |p| p['name'] == crate }&.fetch('version', '?') || '?'
  rescue StandardError
    '?'
  end

  def parse_throughput(output, size)
    parts = output.strip.split
    count = parts[0].to_f
    elapsed = parts[1].to_f
    return nil if elapsed <= 0
    msgs_s = count / elapsed
    mbps = (count * size) / elapsed / 1e6
    { msgs_s: msgs_s, mbps: mbps }
  end

  def parse_latency(output)
    parts = output.strip.split
    {
      p50_us: parts[0].to_f,
      p99_us: parts[1].to_f,
      p999_us: parts[2].to_f,
      max_us: parts[3].to_f,
      iterations: parts[4].to_i,
    }
  end

  def run_throughput_cell(peer, transport, addr, size, duration)
    if transport == 'inproc'
      return run_inproc_cell(peer, addr, size, duration)
    end

    push_pid = spawn_process(peer, 'push', addr, size.to_s)
    sleep 0.15
    output = capture_process(peer, 'pull', addr, size.to_s, duration.to_s)
    kill_process(push_pid)
    parse_throughput(output, size)
  end

  def run_inproc_cell(peer, name, size, duration, subcmd: 'inproc')
    output = capture_process(peer, subcmd, name, size.to_s, duration.to_s)
    parse_throughput(output, size)
  end

  def run_latency_cell(peer, transport, addr, size, iterations: 10_000, warmup: 1_000)
    rep_pid = spawn_process(peer, 'rep', addr, size.to_s)
    sleep 0.2
    output = capture_process(
      peer, 'req', addr, size.to_s, iterations.to_s, warmup.to_s
    )
    kill_process(rep_pid)
    parse_latency(output)
  end

  def spawn_process(bin, *args)
    pid = Process.spawn(bin, *args, [:out, :err] => '/dev/null')
    pid
  end

  def capture_process(bin, *args)
    IO.popen([bin, *args], err: '/dev/null', &:read)
  end

  def kill_process(pid)
    Process.kill('TERM', pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    # already exited
  end

  COMPARISON_SIZES = [8, 32, 128, 512, 2048, 8192, 32_768, 131_072, 524_288].freeze
  CHART_COMPARISON_SIZES = [
    8,
    16,
    32,
    64,
    128,
    256,
    512,
    1_024,
    2_048,
    4_096,
    8_192,
    16_384,
    32_768,
    65_536,
    131_072,
    262_144,
  ].freeze
  LATENCY_SIZES = [8, 32, 128, 512, 2_048, 8_192, 32_768, 131_072].freeze
  CHART_LATENCY_SIZES = [
    8,
    16,
    32,
    64,
    128,
    256,
    512,
    1_024,
    2_048,
    4_096,
    8_192,
    16_384,
    32_768,
    65_536,
    131_072,
  ].freeze
end
