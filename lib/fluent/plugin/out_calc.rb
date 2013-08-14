# encoding: UTF-8
class Fluent::CalcOutput < Fluent::Output
  Fluent::Plugin.register_output('calc', self)

  config_param :sum, :string, :default => nil
  config_param :max, :string, :default => nil
  config_param :min, :string, :default => nil
  config_param :avg, :string, :default => nil
  config_param :interval, :time, :default => 5
  config_param :tag, :string, :default => nil
  config_param :add_tag_prefix, :string, :default => 'calc'
  config_param :aggregate, :string, :default => 'tag'
  config_param :store_file, :string, :default => nil

  attr_accessor :counts
  attr_accessor :matches
  attr_accessor :last_checked

  def configure(conf)
    super

    @interval = @interval.to_i
    @sum = Regexp.new(@sum) if @sum
    @max = Regexp.new(@max) if @max
    @min = Regexp.new(@min) if @min
    @avg = Regexp.new(@avg) if @avg

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
    load_from_file
    @watcher = Thread.new(&method(:watcher))
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
    store_to_file
  end

  # Called when new line comes. This method actually does not emit
  def emit(tag, es, chain)
    tag = 'all' if @aggregate == 'all'
      
    # calc
    count = 0; matches = {}
    es.each do |time,record|
      record.keys.each do |key|
        if @sum and @sum.match(key)
          matches[key] = (matches[key] ? matches[key] + record[key] : record[key])
        elsif @max and @max.match(key)
          matches[key] = (matches[key] ? [matches[key], record[key]].max : record[key])
        elsif @min and @min.match(key)
          matches[key] = (matches[key] ? [matches[key], record[key]].min : record[key])
        elsif @avg and @avg.match(key)
          matches[key] = (matches[key] ? matches[key] + record[key] : record[key]) # sum yet
        end
      end
      count += 1
    end

    # thread safe merge
    @counts[tag] ||= 0
    @matches[tag] ||= {}
    @mutex.synchronize do
      matches.keys.each do |key|
        if @sum and @sum.match(key)
          @matches[tag][key] = (@matches[tag][key] ? @matches[tag][key] + matches[key] : matches[key])
        elsif @max and @max.match(key)
          @matches[tag][key] = (@matches[tag][key] ? [@matches[tag][key], matches[key]].max : matches[key])
        elsif @min and @min.match(key)
          @matches[tag][key] = (@matches[tag][key] ? [@matches[tag][key], matches[key]].min : matches[key])
        elsif @avg and @avg.match(key)
          @matches[tag][key] = (@matches[tag][key] ? @matches[tag][key] + matches[key] : matches[key]) # sum yet
        end
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
    @last_checked = Fluent::Engine.now
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
    output = matches.dup
    output.keys.each do |key|
      if @avg and @avg.match(key)
        output[key] = matches[key] / count.to_f # compute avg
      end
    end
    output
  end

  def store_to_file
    return unless @store_file

    begin
      Pathname.new(@store_file).open('wb') do |f|
        Marshal.dump({
          :counts           => @counts,
          :matches          => @matches,
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

  def load_from_file
    return unless @store_file
    return unless (f = Pathname.new(@store_file)).exist?

    begin
      f.open('rb') do |f|
        stored = Marshal.load(f)
        if stored[:aggregate] == @aggregate and
          stored[:sum] == @sum and
          stored[:max] == @max and
          stored[:min] == @min and
          stored[:avg] == @avg
          @counts = stored[:counts]
          @matches = stored[:matches]
        else
          $log.warn "out_calc: configuration param was changed. ignore stored data"
        end
      end
    rescue => e
      $log.warn "out_calc: Can't load store_file #{e.class} #{e.message}"
    end
  end

end
