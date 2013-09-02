# encoding: UTF-8
class Fluent::CalcOutput < Fluent::Output
  Fluent::Plugin.register_output('calc', self)

  def initialize
    super
    require 'pathname'
  end

  config_param :sum, :string, :default => nil
  config_param :max, :string, :default => nil
  config_param :min, :string, :default => nil
  config_param :avg, :string, :default => nil
  config_param :sum_keys, :string, :default => nil
  config_param :max_keys, :string, :default => nil
  config_param :min_keys, :string, :default => nil
  config_param :avg_keys, :string, :default => nil
  config_param :sum_suffix, :string, :default => ""
  config_param :max_suffix, :string, :default => ""
  config_param :min_suffix, :string, :default => ""
  config_param :avg_suffix, :string, :default => ""
  config_param :interval, :time, :default => 5
  config_param :tag, :string, :default => nil
  config_param :add_tag_prefix, :string, :default => 'calc'
  config_param :aggregate, :string, :default => 'tag'
  config_param :store_file, :string, :default => nil

  attr_accessor :counts
  attr_accessor :matches
  attr_accessor :saved_duration
  attr_accessor :saved_at
  attr_accessor :last_checked

  def configure(conf)
    super

    @interval = @interval.to_i
    @sum = Regexp.new(@sum) if @sum
    @max = Regexp.new(@max) if @max
    @min = Regexp.new(@min) if @min
    @avg = Regexp.new(@avg) if @avg
    @sum_keys = @sum_keys ? @sum_keys.split(',') : []
    @max_keys = @max_keys ? @max_keys.split(',') : []
    @min_keys = @min_keys ? @min_keys.split(',') : []
    @avg_keys = @avg_keys ? @avg_keys.split(',') : []

    unless ['tag', 'all'].include?(@aggregate)
      raise Fluent::ConfigError, "aggregate allows tag/all"
    end

    case @aggregate
    when 'all'
      raise Fluent::ConfigError, "tag must be specified for aggregate all" if @tag.nil?
    end

    @counts = {}
    @matches = {}
    @mutex = Mutex.new
  end

  def start
    super
    load_status(@store_file, @interval) if @store_file
    @watcher = Thread.new(&method(:watcher))
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
    save_status(@store_file) if @store_file
  end

  # Called when new line comes. This method actually does not emit
  def emit(tag, es, chain)
    tag = 'all' if @aggregate == 'all'
      
    # calc
    count = 0; matches = { :sum => {}, :max => {}, :min => {}, :avg => {} }
    es.each do |time, record|
      @sum_keys.each do |key|
        value = record[key].to_f if record[key]
        matches[:sum][key] = sum(matches[:sum][key], value)
      end
      @max_keys.each do |key|
        value = record[key].to_f if record[key]
        matches[:max][key] = max(matches[:max][key], value)
      end
      @min_keys.each do |key|
        value = record[key].to_f if record[key]
        matches[:min][key] = min(matches[:min][key], value)
      end
      @avg_keys.each do |key|
        value = record[key].to_f if record[key]
        matches[:avg][key] = avg(matches[:avg][key], value)
      end
      record.keys.each do |key|
        value = record[key].to_f
        if @sum and @sum.match(key)
          matches[:sum][key] = sum(matches[:sum][key], value)
        end
        if @max and @max.match(key)
          matches[:max][key] = max(matches[:max][key], value)
        end
        if @min and @min.match(key)
          matches[:min][key] = min(matches[:min][key], value)
        end
        if @avg and @avg.match(key)
          matches[:avg][key] = sum(matches[:avg][key], value) # sum yet
        end
      end if @sum || @max || @min || @avg
      count += 1
    end

    # thread safe merge
    @counts[tag] ||= 0
    @matches[tag] ||= { :sum => {}, :max => {}, :min => {}, :avg => {} }
    @mutex.synchronize do
      matches[:sum].keys.each do |key|
        @matches[tag][:sum][key] = sum(@matches[tag][:sum][key], matches[:sum][key])
      end
      matches[:max].keys.each do |key|
        @matches[tag][:max][key] = max(@matches[tag][:max][key], matches[:max][key])
      end
      matches[:min].keys.each do |key|
        @matches[tag][:min][key] = min(@matches[tag][:min][key], matches[:min][key])
      end
      matches[:avg].keys.each do |key|
        @matches[tag][:avg][key] = sum(@matches[tag][:avg][key], matches[:avg][key]) # sum yet
      end
      @counts[tag] += count
    end

    chain.next
  rescue => e
    $log.warn "#{e.class} #{e.message} #{e.backtrace.first}"
  end

  # thread callback
  def watcher
    # instance variable, and public accessable, for test
    @last_checked ||= Fluent::Engine.now
    while true
      sleep 0.5
      begin
        if Fluent::Engine.now - @last_checked >= @interval
          now = Fluent::Engine.now
          flush_emit(now - @last_checked)
          @last_checked = now
        end
      rescue => e
        $log.warn "#{e.class} #{e.message} #{e.backtrace.first}"
      end
    end
  end

  # This method is the real one to emit
  def flush_emit(step)
    time = Fluent::Engine.now
    flushed_counts, flushed_matches, @counts, @matches = @counts, @matches, {}, {}

    flushed_counts.keys.each do |tag|
      count = flushed_counts[tag]
      matches = flushed_matches[tag]
      output = generate_output(count, matches)
      tag = @tag ? @tag : "#{@add_tag_prefix}.#{tag}"
      Fluent::Engine.emit(tag, time, output) if output
    end
  end

  def generate_output(count, matches)
    return nil if matches.empty?
    output = {}
    matches[:sum].keys.each do |key|
      output[key + @sum_suffix] = matches[:sum][key]
    end
    matches[:max].keys.each do |key|
      output[key + @max_suffix] = matches[:max][key]
    end
    matches[:min].keys.each do |key|
      output[key + @min_suffix] = matches[:min][key]
    end
    matches[:avg].keys.each do |key|
      output[key + @avg_suffix] = matches[:avg][key] / count.to_f # compute avg here
    end
    output
  end

  def sum(a, b)
    [a, b].compact.inject(:+)
  end

  def max(a, b)
    [a, b].compact.max
  end

  def min(a, b)
    [a, b].compact.min
  end

  # Store internal status into a file
  #
  # @param [String] file_path
  def save_status(file_path)
    return unless file_path

    begin
      Pathname.new(file_path).open('wb') do |f|
        @saved_at = Fluent::Engine.now
        @saved_duration = @saved_at - @last_checked
        Marshal.dump({
          :counts           => @counts,
          :matches          => @matches,
          :saved_at         => @saved_at,
          :saved_duration   => @saved_duration,
          :aggregate        => @aggregate,
          :sum              => @sum,
          :max              => @max,
          :min              => @min,
          :avg              => @avg,
        }, f)
      end
    rescue => e
      $log.warn "out_calc: Can't write store_file #{e.class} #{e.message}"
    end
  end

  # Load internal status from a file
  #
  # @param [String] file_path
  # @param [Interger] interval
  def load_status(file_path, interval)
    return unless (f = Pathname.new(file_path)).exist?

    begin
      f.open('rb') do |f|
        stored = Marshal.load(f)
        if stored[:aggregate] == @aggregate and
          stored[:sum] == @sum and
          stored[:max] == @max and
          stored[:min] == @min and
          stored[:avg] == @avg

          if !stored[:matches].empty? and !stored[:matches].first[1].has_key?(:max)
            $log.warn "out_calc: stored data does not have compatibility with the current version. ignore stored data"
            return
          end

          if Fluent::Engine.now <= stored[:saved_at] + interval
            @counts = stored[:counts]
            @matches = stored[:matches]
            @saved_at = stored[:saved_at]
            @saved_duration = stored[:saved_duration]

            # skip the saved duration to continue counting
            @last_checked = Fluent::Engine.now - @saved_duration
          else
            $log.warn "out_calc: stored data is outdated. ignore stored data"
          end
        else
          $log.warn "out_calc: configuration param was changed. ignore stored data"
        end
      end
    rescue => e
      $log.warn "out_calc: Can't load store_file #{e.class} #{e.message}"
    end
  end

end
