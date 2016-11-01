module Cassandra
  module Utils
    module Stats
      class Health < Utils::CLI::Base
        def run!
          running = true
          running &= nodetool_statusgossip.strip == 'running'
          running &= nodetool_statusthrift.strip == 'running'
          running &= state == :normal
          running = to_dd running
          push_metric running
          running
        end

        def metric_name
          'cassandra.service.running'
        end

        # Return the state of the Cassandra node
        #
        # The returned state is reported by "nodetool netstats".
        #
        # @return [state, nil]
        #
        def state
          results = (nodetool_netstats || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Mode:' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase.to_sym
        end

        private

        # Run the "nodetool statusgossip' command and return the output
        #
        # @return [String, nil] Output from the "nodetool statusgossip" command
        #
        def nodetool_statusgossip
          @nodetool_statusgossip ||= DaemonRunner::ShellOut.new(command: 'nodetool statusgossip')
          @nodetool_statusgossip.run!
          @nodetool_statusgossip.stdout
        end

        # Run the "nodetool statusthrift' command and return the output
        #
        # @return [String, nil] Output from the "nodetool statusthrift" command
        #
        def nodetool_statusthrift
          @nodetool_statusthrift||= DaemonRunner::ShellOut.new(command: 'nodetool statusthrift')
          @nodetool_statusthrift.run!
          @nodetool_statusthrift.stdout
        end

        # Run the "nodetool netstats' command and return the output
        #
        # @return [String, nil] Output from the "nodetool netstats" command
        #
        def nodetool_netstats
          @nodetool_netstats ||= DaemonRunner::ShellOut.new(command: 'nodetool netstats', timeout: 120)
          @nodetool_netstats.run!
          @nodetool_netstats.stdout
        end
      end
    end
  end
end
