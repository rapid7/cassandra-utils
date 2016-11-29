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
   end
  end
end
