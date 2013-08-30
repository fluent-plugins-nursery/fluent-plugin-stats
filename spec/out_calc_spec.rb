# encoding: UTF-8
require_relative 'spec_helper'

class Fluent::Test::OutputTestDriver
  def emit_with_tag(record, time=Time.now, tag = nil)
    @tag = tag if tag
    emit(record, time)
  end
end

describe Fluent::CalcOutput do
  before { Fluent::Test.setup }
  CONFIG = %[]
  let(:tag) { 'foo.bar' }
  let(:driver) { Fluent::Test::OutputTestDriver.new(Fluent::CalcOutput, tag).configure(config) }

  describe 'test configure' do
    describe 'bad configuration' do
      context 'invalid aggregate' do
        let(:config) do
          CONFIG + %[
            aggregate foo
          ]
        end
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end

      context 'no tag for aggregate all' do
        let(:config) do
          CONFIG + %[
            aggregate all
          ]
        end
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end
    end

    describe 'good configuration' do
      context "nothing" do
        let(:config) { '' }
        it { expect { driver }.to_not raise_error(Fluent::ConfigError) }
      end

      context 'sum/max/min/avg' do
        let(:config) do
          CONFIG + %[
            sum _count$
            max _max$
            min _min$
            avg _avg$
          ]
        end
        it { expect { driver }.to_not raise_error(Fluent::ConfigError) }
      end

      context "check default" do
        subject { driver.instance }
        let(:config) { CONFIG }
        its(:interval) { should == 5 }
        its(:tag) { should be_nil }
        its(:add_tag_prefix) { should == 'calc' }
        its(:aggregate) { should == 'tag' }
      end
    end
  end

  describe 'test emit' do
    let(:time) { Time.now.to_i }
    let(:messages) do
      [
        {"4xx_count"=>1,"5xx_count"=>2,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3},
        {"4xx_count"=>2,"5xx_count"=>2,"reqtime_max"=>5,"reqtime_min"=>2,"reqtime_avg"=>2},
        {"4xx_count"=>3,"5xx_count"=>2,"reqtime_max"=>1,"reqtime_min"=>3,"reqtime_avg"=>4},
      ]
    end
    let(:emit) do
      driver.run { messages.each {|message| driver.emit(message, time) } }
      driver.instance.flush_emit(0)
    end

    context 'sum/max/min/avg' do
      let(:config) do
        CONFIG + %[
          sum _count$
          max _max$
          min _min$
          avg _avg$
        ]
      end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("calc.#{tag}", time, {
          "4xx_count"=>6,"5xx_count"=>6,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3.0
        })
      end
      it { emit }
    end

    context 'sum/max/min/avg_suffix' do
      let(:config) do
        CONFIG + %[
          sum ^(reqtime|reqsize)$
          max ^reqtime$
          min ^reqtime$
          avg ^reqtime$
          sum_suffix _sum
          max_suffix _max
          min_suffix _min
          avg_suffix _avg
        ]
      end
      let(:messages) do
        [
          {"reqtime"=>1.000,"reqsize"=>10},
          {"reqtime"=>2.000,"reqsize"=>20},
        ]
    end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("calc.#{tag}", time, {
          "reqtime_sum"=>3.000,"reqtime_max"=>2.000,"reqtime_min"=>1.000,"reqtime_avg"=>1.500,"reqsize_sum"=>30
        })
      end
      it { emit }
    end

    context 'tag' do
      let(:config) do
        CONFIG + %[
          tag foo
          sum _count$
        ]
      end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("foo", time, {
          "4xx_count"=>6,"5xx_count"=>6
        })
      end
      it { emit }
    end

    context 'add_tag_prefix' do
      let(:config) do
        CONFIG + %[
          add_tag_prefix foo
          sum _count$
        ]
      end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("foo.#{tag}", time, {
          "4xx_count"=>6,"5xx_count"=>6
        })
      end
      it { emit }
    end

    context 'aggregate' do
      let(:emit) do
        driver.run { messages.each {|message| driver.emit_with_tag(message, time, 'foo.bar') } }
        driver.run { messages.each {|message| driver.emit_with_tag(message, time, 'foo.bar2') } }
        driver.instance.flush_emit(0)
      end

      context 'aggregate all' do
        let(:config) do
          CONFIG + %[
          aggregate all
          tag foo
          sum _count$
          max _max$
          min _min$
          avg _avg$
        ]
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, {
            "4xx_count"=>12,"5xx_count"=>12,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3.0
          })
        end
        it { emit }
      end

      context 'aggregate tag' do
        let(:config) do
          CONFIG + %[
          aggregate tag
          add_tag_prefix calc
          sum _count$
          max _max$
          min _min$
          avg _avg$
          ]
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("calc.foo.bar", time, {
            "4xx_count"=>6,"5xx_count"=>6,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3.0
          })
          Fluent::Engine.should_receive(:emit).with("calc.foo.bar2", time, {
            "4xx_count"=>6,"5xx_count"=>6,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3.0
          })
        end
        it { emit }
      end
    end

    describe "store_file" do
      let(:store_file) do
        dirname = "tmp"
        Dir.mkdir dirname unless Dir.exist? dirname
        filename = "#{dirname}/test.dat"
        File.unlink filename if File.exist? filename
        filename
      end

      let(:config) do
        CONFIG + %[
          sum _count$
          store_file #{store_file}
        ]
      end

      it 'stored_data and loaded_data should equal' do
        driver.run { messages.each {|message| driver.emit(message, time) } }
        driver.instance.shutdown
        stored_counts = driver.instance.counts
        stored_matches = driver.instance.matches
        stored_saved_at = driver.instance.saved_at
        stored_saved_duration = driver.instance.saved_duration
        driver.instance.counts = {}
        driver.instance.matches = {}
        driver.instance.saved_at = nil
        driver.instance.saved_duration = nil

        driver.instance.start
        loaded_counts = driver.instance.counts
        loaded_matches = driver.instance.matches
        loaded_saved_at = driver.instance.saved_at
        loaded_saved_duration = driver.instance.saved_duration

        loaded_counts.should == stored_counts
        loaded_matches.should == stored_matches
        loaded_saved_at.should == stored_saved_at
        loaded_saved_duration.should == stored_saved_duration
      end
    end
  end
end

