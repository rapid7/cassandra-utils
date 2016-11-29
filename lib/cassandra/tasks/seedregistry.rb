require 'daemon_runner/shell_out'

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
   end
  end
end
