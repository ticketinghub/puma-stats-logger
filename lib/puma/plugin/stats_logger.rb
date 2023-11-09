# coding: utf-8
# frozen_string_literal: true
require "puma"
require "puma/plugin"

# Wrap puma's stats in a safe API
class PumaStats
  def initialize(stats, previous_requests_count = 0)
    @stats = stats
    @previous_requests_count = previous_requests_count
  end

  def clustered?
    @stats.has_key?(:workers)
  end

  def workers
    @stats.fetch(:workers, 1)
  end

  def booted_workers
    @stats.fetch(:booted_workers, 1)
  end

  def old_workers
    @stats.fetch(:old_workers, 0)
  end

  def running_workers
    if clustered?
      @stats[:worker_status].count { |s| s[:last_status].fetch(:running, 0) > 0 }
    else
      @stats.fetch(:running, 0) > 0
    end
  end

  def busy_workers
    if clustered?
      @stats[:worker_status].count { |s| busy_worker?(s) }
    else
      ((@stats.fetch(:max_threads, 0) - @stats.fetch(:pool_capacity, 0)) > 0) ? 1 : 0
    end
  end

  def idle_workers
    booted_workers - busy_workers
  end

  def running_threads
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:running, 0) }.inject(0, &:+)
    else
      @stats.fetch(:running, 0)
    end
  end

  def backlog
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:backlog, 0) }.inject(0, &:+)
    else
      @stats.fetch(:backlog, 0)
    end
  end

  def pool_capacity
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:pool_capacity, 0) }.inject(0, &:+)
    else
      @stats.fetch(:pool_capacity, 0)
    end
  end

  def max_threads
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:max_threads, 0) }.inject(0, &:+)
    else
      @stats.fetch(:max_threads, 0)
    end
  end

  def requests_count
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:requests_count, 0) }.inject(0, &:+)
    else
      @stats.fetch(:requests_count, 0)
    end
  end

  def percent_busy_threads
    (1 - (idle_threads / max_threads.to_f)) * 100
  end

  def busy_threads
    max_threads - idle_threads
  end

  def idle_threads
    pool_capacity
  end

  def requests_delta
    requests_count - @previous_requests_count
  end

  private

  def busy_worker?(worker_stats)
    busy_threads_for_worker(worker_stats) > 0
  end

  def busy_threads_for_worker(worker_stats)
    worker_stats[:last_status].fetch(:max_threads, 0) - worker_stats[:last_status].fetch(:pool_capacity, 0)
  end
end


class PumaStatsLogger
  def initialize(log_writer)
    @log_writer = log_writer
  end

  def log(stats)
    @log_writer.log(build_stats_log_entry(stats))
  end

  private

  def build_stats_log_entry(stats)
    entry = String.new("Puma Stats: ")
    entry << "puma.workers=#{stats.workers} "
    entry << "puma.booted_workers=#{stats.booted_workers} "
    entry << "puma.running_workers=#{stats.running_workers} "
    entry << "puma.busy_workers=#{stats.busy_workers} "
    entry << "puma.idle_workers=#{stats.idle_workers} "
    entry << "puma.running_threads=#{stats.running_threads} "
    entry << "puma.busy_threads=#{stats.busy_threads} "
    entry << "puma.idle_threads=#{stats.idle_threads} "
    entry << "puma.percent_busy_threads=#{stats.percent_busy_threads.round(2) } "
    entry << "puma.backlog=#{stats.backlog} "
    entry << "puma.max_threads=#{stats.max_threads} "
    entry << "puma.requests_count=#{stats.requests_count} "
    entry
  end
end

Puma::Plugin.create do
  # We can start doing something when we have a launcher:
  def start(launcher)
    @launcher = launcher
    @log_writer =
      if Gem::Version.new(Puma::Const::PUMA_VERSION) >= Gem::Version.new(6)
        @launcher.log_writer
      else
        @launcher.events
      end

    @stats_logger = ::PumaStatsLogger.new(@log_writer)
    @interval_seconds = ENV.fetch("PUMA_STATS_INTERVAL_SECONDS", "20").to_f
    @log_writer.debug "Puma Stats Logger: enabled (interval: #{@interval}s)"

    register_hooks
  end

  private

  def register_hooks
    in_background(&method(:stats_loop))
  end

  def prefixed_metric_name(puma_metric)
    "#{@metric_prefix}#{puma_metric}"
  end

  # Send data to statsd every few seconds
  def stats_loop
    previous_requests_count = 0

    sleep 5
    loop do
      @log_writer.debug "statsd: notify statsd"
      begin
        stats = ::PumaStats.new(Puma.stats_hash, previous_requests_count)
        previous_requests_count = stats.requests_count
        @stats_logger.log(stats)
      rescue StandardError => e
        @log_writer.unknown_error e, nil, "! Puma Stats Logger: logging stats failed"
      ensure
        sleep @interval_seconds
      end
    end
  end
end
