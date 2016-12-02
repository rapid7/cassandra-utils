require 'daemon_runner/shell_out'
require 'daemon_runner/semaphore'

module Cassandra
  module Tasks
   class SeedRegistry
     # Create a new SeedRegistry task
     #
     # @param cluster_name [String] unique name (in Consul) for the Cassandra cluster
     #
     # @return [SeedRegistry]
     #
     def initialize cluster_name
       @cluster_name = cluster_name.to_s
       raise ArgumentError.new('cluster_name must not be empty') if @cluster_name.empty?
       @semaphore = nil
       @renew_thread = nil
     end

     # Get a lock in Consul registering the Cassandra node as a seed
     #
     def run!
       if can_seed?
         try_get_seed_lock
       else
         release_seed_lock
       end
     end

     # Return true if the Cassandra node is a valid seed, false otherwise
     #
     # @return [Boolean]
     #
     def can_seed?
       return false unless state == :normal

       results = (nodetool_info || '').split("\n")
       results.map! { |line| line.strip }

       filter_results = lambda do |key|
         potential = results.select { |line| line.include? key }
         potential.map! { |line| line.split(':')[1] }
         potential.compact!
         potential.size == 1 && potential.first.strip == 'true'
       end

       return false unless filter_results.call('Gossip active')
       return false unless filter_results.call('Thrift active')
       return false unless filter_results.call('Native Transport active')

       true
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

     # Return the data center the Cassandra node is in
     #
     # The returned data center is reported by "nodetool info".
     #
     # @return [String, nil]
     #
     def data_center
       results = (nodetool_info || '').split("\n")
       results.map! { |line| line.strip }
       results.select! { |line| line.include?('Data Center') }
       results.map! { |line| line.split(':')[1] }
       results.compact!
       return nil if results.size != 1
       results.first.strip
     end

     # Return the rack the Cassandra node is in
     #
     # The returned rack is reported by "nodetool info".
     #
     # @return [String, nil]
     #
     def rack
       results = (nodetool_info || '').split("\n")
       results.map! { |line| line.strip }
       results.select! { |line| line.include?('Rack') }
       results.map! { |line| line.split(':')[1] }
       results.compact!
       return nil if results.size != 1
       results.first.strip
     end

     private

     # Run the "nodetool info" command and return the output
     #
     # @return [String, nil] Output from the "nodetool info" command
     #
     def nodetool_info
       @nodetool_info ||= DaemonRunner::ShellOut.new(command: 'nodetool info', timeout: 300)
       @nodetool_info.run!
       @nodetool_info.stdout
     end

     # Run the "nodetool netstats" command and return the output
     #
     # @return [String, nil] Output from the "nodetool netstats" command
     #
     def nodetool_netstats
       @nodetool_netstats ||= DaemonRunner::ShellOut.new(command: 'nodetool netstats', timeout: 300)
       @nodetool_netstats.run!
       @nodetool_netstats.stdout
     end

     # Try to get the lock in Consul for this node as a seed
     #
     def try_get_seed_lock
       if @semaphore.nil?
         name = "#{@cluster_name}/#{data_center}-#{rack}"
         @semaphore = DaemonRunner::Semaphore.lock(name, 1)
       end

       if @renew_thread.nil?
         @renew_thread = @semaphore.renew
       end
     end

     # Release the lock in Consul for this node as a seed
     #
     def release_seed_lock
       unless @renew_thread.nil?
         @renew_thread.kill
         @renew_thread = nil
       end

       unless @semaphore.nil?
         while @semaphore.locked?
           @semaphore.try_release
           sleep 0.1
         end
         @semaphore = nil
       end
     end
   end
  end
end
