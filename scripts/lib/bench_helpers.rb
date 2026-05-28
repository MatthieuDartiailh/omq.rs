# frozen_string_literal: true

require 'json'

module BenchHelpers
  SIZE_LABELS = {
    8         => '8 B',
    16        => '16 B',
    32        => '32 B',
    64        => '64 B',
    128       => '128 B',
    256       => '256 B',
    512       => '512 B',
    1_024     => '1 KiB',
    2_048     => '2 KiB',
    4_096     => '4 KiB',
    8_192     => '8 KiB',
    16_384    => '16 KiB',
    32_768    => '32 KiB',
    65_536    => '64 KiB',
    131_072   => '128 KiB',
    262_144   => '256 KiB',
    524_288   => '512 KiB',
  }.freeze

  TABLE_SIZES = [32, 1_024, 4_096].freeze

  module_function

  def size_label(n)
    SIZE_LABELS[n] || "#{n} B"
  end

  def format_si(v, nil_str: nil)
    return nil_str unless v && v > 0
    if    v >= 1e6   then '%.2fM' % (v / 1e6)
    elsif v >= 100e3 then '%.0fk' % (v / 1e3)
    elsif v >= 1e3   then '%.1fk' % (v / 1e3)
    else                  '%.0f'  % v
    end
  end

  def format_mbps(v, nil_str: nil)
    return nil_str unless v && v > 0
    if    v >= 10_000 then '%.1f GB/s' % (v / 1_000.0)
    elsif v >= 1_000  then '%.2f GB/s' % (v / 1_000.0)
    elsif v >= 100    then '%.0f MB/s' % v
    elsif v >= 10     then '%.1f MB/s' % v
    else                   '%.2f MB/s' % v
    end
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

  def format_mbps_bw(v, nil_str: nil)
    return nil_str unless v && v > 0
    v >= 1_000 ? '%.1f GB/s' % (v / 1_000.0) : '%.0f MB/s' % v
  end

  def speedup_str(val, ref_val)
    return '—' unless val && ref_val && ref_val > 0
    r = val.to_f / ref_val
    r >= 1.1 ? '**%.1f×**' % r : '%.2f×' % r
  end

  def latency_speedup_str(ref_val, val)
    return '—' unless val && ref_val && val > 0
    r = ref_val.to_f / val
    r >= 1.1 ? '**%.1f×**' % r : '%.2f×' % r
  end

  def load_jsonl(path, exclude_runs: [])
    return [] unless File.exist?(path)
    rows = File.readlines(path, chomp: true).filter_map do |line|
      next if line.strip.empty?
      JSON.parse(line, symbolize_names: true) rescue nil
    end
    rows.reject! { |r| exclude_runs.include?(r[:run_id]) } unless exclude_runs.empty?
    rows
  end

  def replace_block(text, marker, content)
    b = "<!-- BEGIN #{marker} -->"
    e = "<!-- END #{marker} -->"
    re = /#{Regexp.escape(b)}.*?#{Regexp.escape(e)}/m
    abort "Marker #{b} not found" unless text.match?(re)
    text.sub(re, "#{b}\n#{content}#{e}")
  end

  def latest_row(rows, pattern:, transport:, peers:, msg_size:)
    rows.reverse_each.find do |r|
      r[:pattern]   == pattern   &&
        r[:transport] == transport &&
        r[:peers]     == peers     &&
        r[:msg_size]  == msg_size
    end
  end

  # Generic markdown table: Size rows, arbitrary columns.
  #
  # columns:   column headers (transport names, mechanism names, etc.)
  # cell_fmt:  ->(row_or_nil) { "cell" }
  # lookup:    ->(column, msg_size) { row_hash_or_nil }
  # empty_msg: shown when no data exists
  # col_align: separator cell (default '---', use '---:' for right-align)
  #
  # Returns markdown ending with \n\n (blank line before END marker).
  def build_size_table(columns:, cell_fmt:, lookup:, empty_msg:, col_align: '---', sizes: nil)
    allowed = sizes || SIZE_LABELS.keys
    live_cols = columns.select { |c| allowed.any? { |s| lookup.call(c, s) } }
    sizes = allowed.select { |s| live_cols.any? { |c| lookup.call(c, s) } }
    return "(#{empty_msg})\n" if live_cols.empty?

    out = +""
    out << "| Size | #{live_cols.join(' | ')} |\n"
    out << "|---|#{live_cols.map { col_align }.join('|')}|\n"
    sizes.each do |sz|
      cells = live_cols.map { |c| cell_fmt.call(lookup.call(c, sz)) }
      out << "| #{size_label(sz)} | #{cells.join(' | ')} |\n"
    end
    out << "\n"
    out
  end
end
