require 'socket'
require 'json'
require 'time'
require 'set'
require 'tmpdir'
require_relative 'version'

module Cassandra
  module Utils
   class Autoclean
     # @return [String] the path on disk where tokens will be cached
     attr_reader :token_cache_path

     # Create a new Autoclean task
     #
     # @param options [Object] optional configuration settings
     # (see #token_cache_path)
     #
     # @return [Autoclean]
     #
     def initialize(options = {})
       @token_cache_path = options[:token_cache_path]
       @token_cache_path ||= File.join(Dir.tmpdir, 'autoclean-tokens.json')
     end

     # Schedule the Cassandra cleanup process to run daily
     #
     def schedule
       [:interval, '1d']
     end

     # Run the Cassandra cleanup process if necessary
     #
     def run!
       new_tokens = Set.new tokens
       old_tokens = Set.new cached_tokens
       if new_tokens != old_tokens
         status = nodetool_cleanup
         save_tokens if !status.nil? && status.exitstatus == 0
       end
     end

     # Get the cached tokens this node owns
     #
     # @return [Array<String>] Cached tokens
     #
     def cached_tokens
       data = File.read token_cache
       data = JSON.parse data
       return [] unless data['version'] == ::Cassandra::Utils::VERSION

       tokens = data['tokens']
       return [] if tokens.nil?
       return [] unless tokens.respond_to? :each

       tokens.sort!
       tokens
     # Token file could not be opend or parsed
     rescue Errno::ENOENT, JSON::ParserError
       []
     end

     # Save the list of tokens this node owns to disk
     # These can be read by `cached_tokens`
     #
     def save_tokens
       data = {
         :timestamp => Time.now.iso8601,
         :tokens => tokens,
         :version => ::Cassandra::Utils::VERSION
       }

       token_cache.write data.to_json
       token_cache.flush
     end

     # Get the tokens this node owns
     #
     # The "nodetool ring" command returns
     #
     # Address    Rack  Status  State   Load  Size  Owns  Token
     # 127.0.0.1  r1    Up      Normal  10    GB    33%   123456789
     #
     # @return [Array<String>] Tokens owned by this node
     #
     def tokens
       return [] if address.nil?
       results = (nodetool_ring || '').split("\n")
       results.map! { |line| line.strip }
       results.select! { |line| line.start_with? address }
       results.map! { |line| line.split(/\s+/)[7] }
       results.compact!
       results.sort
     end

     # Get the IP address of this node
     #
     # @return [String, nil] IP address of this node
     #
     def address
       if @address.nil?
         addr = Socket.ip_address_list.find { |addr| addr.ipv4_private? }
         @address = addr.ip_address unless addr.nil?
       end
       @address
     end

     private

     # Run the "nodetool ring" command and return the output
     #
     # @return [String, nil] Output from the "nodetool ring" command
     #
     def nodetool_ring
       @nodetool_ring ||= Mixlib::ShellOut.new('nodetool ring', {
         :cwd => '/tmp',
         :timeout => 120
       })
       @nodetool_ring.run_command
       @nodetool_ring.error!
       @nodetool_ring.stdout
     end

     # Get the status of a "nodetool cleanup" command
     #
     # This will atempt to track a running "nodetool cleanup" process if one's
     # found. If a running process isn't found, a new process will be launched.
     #
     # @return [Process::Status, nil]
     #
     def nodetool_cleanup
       pid = find_nodetool_cleanup
       pid = exec_nodetool_cleanup if pid.nil?
       wait_nodetool_cleanup pid
     end

     # Get the ID of the first running "nodetool cleanup" process found
     #
     # @return [Integer, nil]
     #
     def find_nodetool_cleanup
       pids = `pgrep -f 'nodetool cleanup'`.strip.split "\n"
       return nil if pids.empty?
       pids.first.to_i
     end

     # Run "nodetool cleanup" command
     #
     # @return [Integer] ID of the "nodetool cleanup" command
     #
     def exec_nodetool_cleanup
       # The `pgroup: true` option spawns cleanup in its own process group.
       # So if this process dies, cleanup continues to run.
       Process.spawn('nodetool', 'cleanup', pgroup: true)
     end

     # Wait for a "nodetool cleanup" process to exit
     #
     # This handles the `SystemCallError` that's raised if no child process is
     # found. In that case, the returned status will be `nil`.
     #
     # @return [Process::Status, nil] status
     #
     def wait_nodetool_cleanup pid
       pid, status = Process.wait2(pid, Process::WUNTRACED)
       status
     rescue Errno::ECHILD
       nil
     end

     # Get the cache tokens wil be saved in
     #
     # @return [File] File where tokens wil be saved
     #
     def token_cache
       File.new(token_cache_path)
     end
   end
  end
end
