module Cassandra
  module Utils
    module Stats
      class Health < Utils::CLI::Base
        def run!
          running = true
          if state == :normal
            running &&= gossipstate == 'true'
            running &&= thriftstate == 'true'
          end
          Utils::Statsd.new(metric_name).to_dd(running).push!
          running
        end

        def metric_name
          'cassandra.service.running'
        end

        # Return the state of nodetool status
        #
        # The returned state is reported by "nodetool status".
        #


        # Return the state of nodetool info gossip
        #
        # The returned state is reported by "nodetool info".
        #
        # @return [String, nil]
       def gossipstate
          results = (nodetool_info || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Gossip active' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase
        end


        # Return the state of nodetool info thrift
        #
        # The returned state is reported by "nodetool info".
        #
        # @return [String, nil]
        def thriftstate
          results = (nodetool_info || '').split("\n")
          results.map! { |line| line.strip }
          results.select! { |line| line.include? 'Thrift active' }
          results.map! { |line| line.split(':')[1] }
          results.compact!
          return nil if results.size != 1
          results.first.strip.downcase
        end

        # Return the state of the Cassandra node
        #
        # The returned state is reported by "nodetool netstats".
        #
        # @return [Symbol, nil]
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

        def task_id
          ['health', 'nodetool']
        end

        private

        # Shell out via DaemonRunner to run 'nodetool info'
        #
        # The returned state is either true or false
        #
        # @return [String, nil] Output from the "nodetool info" command 
       def nodetool_info
            @nodetool_info ||= DaemonRunner::ShellOut.new(command: 'nodetool info')
            @nodetool_info.run!
            @nodetool_info.stdout
          end

        # Run the "nodetool netstats' command and return the output
        #
        # @return [String, nil] Output from the "nodetool netstats" command
        #
        def nodetool_netstats
          @nodetool_netstats ||= DaemonRunner::ShellOut.new(command: 'nodetool netstats', timeout: 300)
          @nodetool_netstats.run!
          @nodetool_netstats.stdout
        end

        # Shell out via DaemonRunner to run 'nodetool status'
        #
        # The returned state is a value that NEED DEFINE
        #
        # @return [String, nil] Output from the "nodetool status" command

      end
    end
  end
end
