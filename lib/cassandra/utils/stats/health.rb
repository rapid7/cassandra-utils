module Cassandra
  module Utils
    module Stats
      class Health < Utils::CLI::Base
        def run!
          running = true
          running &= nodetool_statusgossip.strip.include?('running')
          running &= nodetool_statusthrift.strip.include?('running')
          to_dd(running)
        end

        def metric_name
          'cassandra.service.running'
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
      end
    end
  end
end
