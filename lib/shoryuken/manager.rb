require 'shoryuken/processor'
require 'shoryuken/fetcher'

module Shoryuken
  class Manager
    include Celluloid
    include Util

    attr_accessor :fetchers, :queues

    trap_exit :processor_died

    def initialize
      @count  = Shoryuken.options[:concurrency] || 25
      @queues = Shoryuken.queues.dup.uniq

      @done = false

      @busy  = []
      @pending = []
      @ready = @count.times.map { Processor.new_link(current_actor) }
    end

    def start
      logger.info 'Starting'

      dispatch
    end

    def stop(options = {})
      watchdog('Manager#stop died') do
        @done = true

        @fetchers.each { |fetcher| fetcher.terminate if fetcher.alive? }

        logger.info { "Shutting down #{@ready.size} quiet workers" }

        @ready.each do |processor|
          processor.terminate if processor.alive?
        end
        @ready.clear

        return after(0) { signal(:shutdown) } if @busy.empty?

        if options[:shutdown]
          hard_shutdown_in(options[:timeout])
        else
          soft_shutdown(options[:timeout])
        end
      end
    end

    def processor_done(queue, processor)
      watchdog('Manager#processor_done died') do
        logger.info "Process done for '#{queue}'"

        @busy.delete processor

        if stopped?
          processor.terminate if processor.alive?
        else
          @ready << processor
        end
      end
    end

    def processor_died(processor, reason)
      watchdog("Manager#processor_died died") do
        logger.info "Process died, reason: #{reason}"

        @busy.delete processor

        unless stopped?
          @ready << Processor.new_link(current_actor)
        end
      end
    end

    def stopped?
      @done
    end

    def assign(queue, sqs_msg)
      watchdog("Manager#assign died") do
        logger.info "Assigning #{sqs_msg.id}"

        processor = @ready.pop

        if processor.nil?
          logger.info "No ready processors, so saving message and pausing queues"
          # I was going to write code to execute these pending jobs later, but it seems to be already happening?
          @pending << [queue, sqs_msg]
          @fetchers_paused = true
        else
          @busy << processor

          processor.async.process(queue, sqs_msg)
        end
      end
    end

    def rebalance_queue_weight!(queue)
      watchdog('Manager#rebalance_queue_weight! died') do
        if (original = original_queue_weight(queue)) > (current = current_queue_weight(queue))
          logger.info "Increasing '#{queue}' weight to #{current + 1}, max: #{original}"

          @queues << queue
        end
      end
    end

    def pause_queue!(queue)
      return if !@queues.include?(queue) || Shoryuken.options[:delay].to_f <= 0

      logger.info "Pausing '#{queue}' for #{Shoryuken.options[:delay].to_f} seconds, because it's empty"

      @queues.delete(queue)

      after(Shoryuken.options[:delay].to_f) { async.restart_queue!(queue) }
    end


    def dispatch
      return if stopped?

      logger.debug { "Ready: #{@ready.size}, Busy: #{@busy.size}, Active Queues: #{unparse_queues(@queues)}" }

      if @ready.empty?
        logger.debug { 'Pausing fetchers, because all processors are busy' }

        after(1) { dispatch }

        return
      end

      @fetchers.each do |fetcher|
        if queue = next_queue
          fetcher.async.fetch(queue, @ready.size)
        else
          logger.debug { 'Pausing fetchers, because all queues are paused' }

          @fetchers_paused = true
        end
      end
    end

    private

    def restart_queue!(queue)
      return if stopped?

      unless @queues.include? queue
        logger.info "Restarting '#{queue}'"

        @queues << queue

        if @fetchers_paused
          logger.debug { 'Restarting fetchers' }

          @fetchers_paused = false

          dispatch
        end
      end
    end

    def current_queue_weight(queue)
      queue_weight(@queues, queue)
    end

    def original_queue_weight(queue)
      queue_weight(Shoryuken.queues, queue)
    end

    def queue_weight(queues, queue)
      queues.count { |q| q == queue }
    end

    def next_queue
      return nil if @queues.empty?

      queue = @queues.shift
      @queues << queue

      queue
    end

    def soft_shutdown(delay)
      logger.info { "Waiting for #{@busy.size} busy workers" }

      if @busy.size > 0
        after(delay) { soft_shutdown(delay) }
      else
        after(0) { signal(:shutdown) }
      end
    end

    def hard_shutdown_in(delay)
      logger.info { "Waiting for #{@busy.size} busy workers" }
      logger.info { "Pausing up to #{delay} seconds to allow workers to finish..." }

      after(delay) do
        watchdog("Manager#hard_shutdown_in died") do
          if @busy.size > 0
            logger.info { "Hard shutting down #{@busy.size} busy workers" }

            @busy.each do |processor|
              t = processor.bare_object.actual_work_thread
              t.raise Shutdown if processor.alive?
            end
          end

          after(0) { signal(:shutdown) }
        end
      end
    end
  end
end
