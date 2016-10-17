require 'socket'

module Cassandra
  module Utils
   class Autoclean
     def run!
     end

     # Get the IP address of this node
     #
     # @return [String, nil] IP address of this node
     #
     def address
       if @address.nil? || @address.empty?
         addr = Socket.ip_address_list.find { |addr| addr.ipv4_private? }
         @address = addr.ip_address unless addr.nil?
       end
       @address
     end
   end
  end
end
