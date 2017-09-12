module Cassandra
  module Utils
    module Stats
      class Health < Utils::CLI::Base
        def run!
          running = true
          if state == :normal
            running &&= gossipstate.strip == 'true'
            running &&= thriftstate.strip == 'true'
          end
          Utils::Statsd.new(metric_name).to_dd(running).push!
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
        def nodetool_info
            @nodetool_info ||= DaemonRunner::ShellOut.new(command: 'nodetool info')
            @nodetool_info.run!
            @nodetool_info.stdout
          end


        def gossipstate
          results = (nodetool_info || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Gossip active    :' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase.to_sym
        end

        def thriftstate
          results = (nodetool_info || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Thrift active    :' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase.to_sym
        end


        def state
          results = (nodetool_netstats || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Mode:' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase.to_sym
        end

        def task_id
          ['health', 'nodetool']
        end

        private

        # Run the "nodetool netstats' command and return the output
        #
        # @return [String, nil] Output from the "nodetool netstats" command
        #
        def nodetool_netstats
          @nodetool_netstats ||= DaemonRunner::ShellOut.new(command: 'nodetool netstats', timeout: 300)
          @nodetool_netstats.run!
          @nodetool_netstats.stdout
        end
      end
    end
  end
end
