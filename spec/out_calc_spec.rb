# encoding: UTF-8
require_relative 'spec_helper'

describe Fluent::CalcOutput do
  before { Fluent::Test.setup }
  CONFIG = %[
    input_key message
  ]
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

    context 'interval' do
      pending
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

    context 'tag' do
      let(:config) do
        CONFIG + %[
          tag foo
        ]
      end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("foo", time, {})
      end
      it { emit }
    end

    context 'add_tag_prefix' do
      let(:config) do
        CONFIG + %[
          add_tag_prefix foo
        ]
      end
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::Engine.should_receive(:emit).with("foo.#{tag}", time, {})
      end
      it { emit }
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
          "4xx_count"=>6,"5xx_count"=>6,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3.0
        })
      end
      it { emit }
    end
  end
end

