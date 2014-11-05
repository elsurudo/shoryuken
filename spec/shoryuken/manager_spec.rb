require 'spec_helper'
require 'shoryuken/manager'

describe Shoryuken::Manager do
  describe 'Auto Scaling' do
    it 'decreases weight' do
      queue1 = 'shoryuken'
      queue2 = 'uppercut'

      Shoryuken.queues.clear
      # [shoryuken, 2]
      # [uppercut,  1]
      Shoryuken.queues << queue1
      Shoryuken.queues << queue1
      Shoryuken.queues << queue2

      expect(subject.instance_variable_get('@queues')).to eq [queue1, queue2]

      subject.pause_queue!(queue1)

      expect(subject.instance_variable_get('@queues')).to eq [queue2]
    end

    it 'increases weight' do
      queue1 = 'shoryuken'
      queue2 = 'uppercut'

      Shoryuken.queues.clear
      # [shoryuken, 3]
      # [uppercut,  1]
      Shoryuken.queues << queue1
      Shoryuken.queues << queue1
      Shoryuken.queues << queue1
      Shoryuken.queues << queue2

      expect(subject.instance_variable_get('@queues')).to eq [queue1, queue2]
      subject.pause_queue!(queue1)
      expect(subject.instance_variable_get('@queues')).to eq [queue2]

      subject.rebalance_queue_weight!(queue1)
      expect(subject.instance_variable_get('@queues')).to eq [queue2, queue1]

      subject.rebalance_queue_weight!(queue1)
      expect(subject.instance_variable_get('@queues')).to eq [queue2, queue1, queue1]

      subject.rebalance_queue_weight!(queue1)
      expect(subject.instance_variable_get('@queues')).to eq [queue2, queue1, queue1, queue1]
    end

    it 'adds queue back' do
      queue1 = 'shoryuken'
      queue2 = 'uppercut'

      Shoryuken.queues.clear
      # [shoryuken, 2]
      # [uppercut,  1]
      Shoryuken.queues << queue1
      Shoryuken.queues << queue1
      Shoryuken.queues << queue2

      Shoryuken.options[:delay] = 0.1

      fetcher = double('Fetcher').as_null_object
      subject.fetchers = [fetcher]

      subject.pause_queue!(queue1)
      expect(subject.instance_variable_get('@queues')).to eq [queue2]

      sleep 0.5

      expect(subject.instance_variable_get('@queues')).to eq [queue2, queue1]
    end
  end
end
