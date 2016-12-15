require 'socket'
require 'json'
require 'time'
require 'set'
require 'tmpdir'
require_relative '../utils/version'

module Cassandra
  module Tasks
   class Autoclean
     include ::DaemonRunner::Logger

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
       @service_name = options[:cleanup_service_name]
       @lock_count = options[:cleanup_lock_count]
       @logger = options[:logger]
     end

     # Schedule the Cassandra cleanup process to run daily
     #
     def schedule
       [:interval, '1d']
     end

     # Return the status of the Cassandra node
     #
     # A node is considered up if it has a status of "Up" as reported by
     # "nodetool status". If multiple nodes with this node's IP address show
     # up in "nodetool status", this node is considered down.
     #
     # @return [:up, :down]
     #
     def status
       return(:down).tap { logger.warn 'Cassandra node is DOWN' } if address.nil?
       results = (nodetool_status || '').split("\n")
       results.map! { |line| line.strip }
       results.select! { |line| line.include? address }
       results.map! { |line| line.split(/\s+/)[0] }
       results.compact!
       return(:down).tap do
         logger.warn "Cannot find the Cassandra node (#{address}) in `nodetool status`"
       end if results.size != 1
       (results.first[0] == 'U') ? :up : :down
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

     # Run the Cassandra cleanup process if necessary
     #
     def run!
       node_status = status
       unless node_status == :up
         logger.debug "Cleanup skipped because node status is not up: #{node_status}"
         return
       end

       node_state = state
       unless node_state == :normal
         logger.debug "Cleanup skipped because node state is not normal: #{node_state}"
         return
       end

       new_tokens = Set.new tokens
       old_tokens = Set.new cached_tokens
       if new_tokens == old_tokens
         logger.debug "Cleanup skipped because tokens haven't changed"
         return
       end

       ::DaemonRunner::Semaphore.lock(@service_name, @lock_count) do
         result = nodetool_cleanup
         save_tokens if !result.nil? && result.exitstatus == 0
       end
     end

     # Get the cached tokens this node owns
     #
     # @return [Array<String>] Cached tokens
     #
     def cached_tokens
       data = token_cache.read
       data = JSON.parse data
       unless data['version'] == ::Cassandra::Utils::VERSION
         logger.debug "Failed to read cached tokens because version didn't match. Expected #{::Cassandra::Utils::VERSION} got #{data['version']}"
         return []
       end

       tokens = data['tokens']
       if tokens.nil?
         logger.debug "Failed to read cached tokens because they're nil"
         return []
       end

       unless tokens.respond_to? :each
         logger.debug "Failed to read cached tokens because they're invalid"
         return []
       end

       tokens.sort!
       tokens
     # Token file could not be opend or parsed
     rescue Errno::ENOENT, JSON::ParserError => e
       logger.debug "Caught exception while reading cached tokens"
       logger.debug e
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
       if address.nil?
         logger.debug "Failed to read live tokens because address is nil"
         return []
       end

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

     def task_id
       ['autoclean', 'nodetool']
     end

     private

     # Run the "nodetool ring" command and return the output
     #
     # @return [String, nil] Output from the "nodetool ring" command
     #
     def nodetool_ring
       @nodetool_ring ||= DaemonRunner::ShellOut.new(command: 'nodetool ring', timeout: 300)
       @nodetool_ring.run!
       @nodetool_ring.stdout
     end

     # Run the "nodetool status' command and return the output
     #
     # @return [String, nil] Output from the "nodetool status" command
     #
     def nodetool_status
       @nodetool_status ||= DaemonRunner::ShellOut.new(command: 'nodetool status', timeout: 300)
       @nodetool_status.run!
       @nodetool_status.stdout
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

     # Get the status of a "nodetool cleanup" command
     #
     # This will atempt to track a running "nodetool cleanup" process if one's
     # found. If a running process isn't found, a new process will be launched.
     #
     # @return [Process::Status, nil]
     #
     def nodetool_cleanup
       pid = find_nodetool_cleanup
       if pid
         logger.debug "Found nodetool cleanup process #{pid} already running"
         Utils::Statsd.new('cassandra.cleanup.running').push!(1)
       end
       pid = exec_nodetool_cleanup if pid.nil?
       if pid
         logger.debug "Started nodetool cleanup process #{pid}"
         Utils::Statsd.new('cassandra.cleanup.running').push!(1)
         status = wait_nodetool_cleanup pid
         logger.debug "Completed nodetool cleanup process #{pid}"
       end
       status
     end

     # Get the ID of the first running "nodetool cleanup" process found
     #
     # @return [Integer, nil]
     #
     def find_nodetool_cleanup
       @pgrep_nodetool_cleanup ||= ::DaemonRunner::ShellOut.new(command: 'pgrep -f "NodeCmd.+cleanu[p]"', valid_exit_codes: [0,1])
       @pgrep_nodetool_cleanup.run!
       pids = @pgrep_nodetool_cleanup.stdout.strip.split "\n"
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
       @nodetool_cleanup ||= ::DaemonRunner::ShellOut.new(command: 'nodetool cleanup', wait: false)
       @nodetool_cleanup.run!
     end

     # Wait for a "nodetool cleanup" process to exit
     #
     # This handles the `SystemCallError` that's raised if no child process is
     # found. In that case, the returned status will be `nil`.
     #
     # @return [Process::Status, nil] status
     #
     def wait_nodetool_cleanup pid
       logger.debug "Waiting for nodetool cleanup process #{pid} to complete"
       ::DaemonRunner::ShellOut.wait2(pid, Process::WUNTRACED)
     end

     # Get the cache tokens wil be saved in
     #
     # @return [File] File where tokens wil be saved
     #
     def token_cache
       File.new(token_cache_path, 'w+')
     end
   end
  end
end
