require 'test_helper'
require 'tempfile'
require 'ostruct'
require 'ipaddr'

# IPV4 regex pulled from Logstash's grok patterns
# https://github.com/logstash-plugins/logstash-patterns-core/blob/v4.0.2/patterns/grok-patterns#L30
IPV4 = Regexp.new('(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])')

class MockShellOut
  attr_reader :stdout

  def initialize(stdout)
    @stdout = stdout
  end

  def run_command
  end

  def error!
  end
end

describe Cassandra::Tasks::Autoclean do
  before do
    @cleaner = Cassandra::Tasks::Autoclean.new
  end

  describe :new do
    it 'defaults to a token cache in $TMPDIR' do
      @cleaner.token_cache_path.must_match /^#{Dir.tmpdir}/
    end

    it 'allows setting a custom token cache' do
      cleaner = Cassandra::Tasks::Autoclean.new(token_cache_path: 'autoclean.json')
      cleaner.token_cache_path.must_match /^autoclean.json$/
    end
  end

  describe :address do
    it 'resolves IPV4 addresses' do
      @cleaner.address.must_match IPV4
    end

    it 'returns nil when addresses do not exist' do
      Socket.stub :ip_address_list, [] do
        @cleaner.address.must_be_nil
      end
    end

    it 'returns the first private IPV4 address' do
      def ipv6 address
        IPAddr.new(address).ipv4_mapped.to_string
      end

      addresses = [
        Addrinfo.tcp('127.0.0.1', 0),      # Loopback IPV4 address
        Addrinfo.tcp(ipv6('52.0.0.1'), 0), # Public IPV6 address
        Addrinfo.tcp('52.0.0.1', 0),       # Public IPV4 address
        Addrinfo.tcp(ipv6('10.0.0.2'), 0), # Private IPV6 address
        Addrinfo.tcp('10.0.0.1', 0),       # Private IPV4 address
        Addrinfo.tcp('10.0.0.2', 0),       # Private IPV4 address
      ]

      Socket.stub :ip_address_list, addresses do
        @cleaner.address.must_equal '10.0.0.1'
      end
    end
  end

  describe :tokens do
    it 'returns no tokens without an address' do
      Socket.stub :ip_address_list,  [] do
        @cleaner.tokens.must_equal []
      end
    end

    it 'returns tokens owned by this node' do
      shellout = lambda do |command, options|
        command.must_equal 'nodetool ring'

        results = <<-END
        Note: Ownership information does not include topology; for complete information, specify a keyspace

        Datacenter: dc1
        ==========
        Address       Rack        Status State   Load            Owns                Token
                                                                                     orphan-token
        10.0.0.1      r1          Up     Normal  1 GB            33%                 2
        10.0.0.1      r1          Up     Normal  1 GB            33%                 1
        10.0.0.3      r3          Up     Joining 1 GB            ?                   5
        10.0.0.2      r2          Down   Normal  1 GB            33%                 4
        10.0.0.1      r1          Up     Normal  1 GB            33%                 3

          Warning: "nodetool ring" is used to output all the tokens of a node.
          To view status related info of a node use "nodetool status" instead.
        END

        MockShellOut.new(results)
      end

      addresses = [Addrinfo.tcp('10.0.0.1', 0)]

      Mixlib::ShellOut.stub :new, shellout do
        Socket.stub :ip_address_list, addresses do
          @cleaner.tokens.must_equal ['1', '2', '3']
        end
      end
    end
  end

  describe :save_tokens do
    it 'saves tokens as JSON to disk' do
      token_cache = Tempfile.new('autoclean')
      tokens = ['6', '7', '8']

      @cleaner.stub :token_cache, token_cache do
        @cleaner.stub :tokens, tokens do
          @cleaner.save_tokens

          data = File.read token_cache
          data = JSON.parse data

          data['tokens'].must_equal ['6', '7', '8']
          data['version'].must_equal ::Cassandra::Utils::VERSION
        end
      end
    end
  end

  describe :cached_tokens do
    it 'returns no tokens if token file does not exist' do
      token_cache = Tempfile.new('autoclean')
      token_cache.close
      token_cache.unlink

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns not tokens if token file fails to parse' do
      token_cache = Tempfile.new('autoclean')
      token_cache.write('')
      token_cache.flush

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns no tokens if token file is corrupt' do
      token_cache = Tempfile.new('autoclean')
      token_cache.write({
        :version => ::Cassandra::Utils::VERSION,
        :tokens => "these are not the tokens you're looking for"
      }.to_json)
      token_cache.flush

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns no tokens if version does not match' do
      token_cache = Tempfile.new('autoclean')
      token_cache.write({
        :version => -1,
        :tokens => ['3', '1', '2']
      }.to_json)
      token_cache.flush

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns sorted cached tokens' do
      token_cache = Tempfile.new('autoclean')
      token_cache.write({
        :version => ::Cassandra::Utils::VERSION,
        :tokens => ['3', '1', '2']
      }.to_json)
      token_cache.flush

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal ['1', '2', '3']
      end
    end
  end

  describe :status do
    stub_nodetool_status = lambda do |command, options|
      command.must_equal 'nodetool status'

      results = <<-END
      Note: Ownership information does not include topology; for complete information, specify a keyspace
      Datacenter: dc1
      ===================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --  Address       Load       Tokens  Owns   Host ID     Rack
      UN  10.0.0.1      1 GB       256     33%    1           r1
      DN  10.0.0.2      1 GB       256     33%    2           r2
      UN  10.0.0.3      1 GB       256     33%    3           r3
      UN  10.0.0.3      1 GB       256     33%    3           r3
      END

      MockShellOut.new(results)
    end

    it 'is up when node is up' do
      addresses = [Addrinfo.tcp('10.0.0.1', 0)]

      Mixlib::ShellOut.stub :new, stub_nodetool_status do
        Socket.stub :ip_address_list, addresses do
          @cleaner.status.must_equal :Up
        end
      end
    end

    it 'is down when node is down' do
      addresses = [Addrinfo.tcp('10.0.0.2', 0)]

      Mixlib::ShellOut.stub :new, stub_nodetool_status do
        Socket.stub :ip_address_list, addresses do
          @cleaner.status.must_equal :Down
        end
      end
    end

    it 'is down when status is ambiguous' do
      addresses = [Addrinfo.tcp('10.0.0.3', 0)]

      Mixlib::ShellOut.stub :new, stub_nodetool_status do
        Socket.stub :ip_address_list, addresses do
          @cleaner.status.must_equal :Down
        end
      end
    end

    it 'is down when node address is not found' do
      addresses = [Addrinfo.tcp('10.0.0.4', 0)]

      Mixlib::ShellOut.stub :new, stub_nodetool_status do
        Socket.stub :ip_address_list, addresses do
          @cleaner.status.must_equal :Down
        end
      end
    end
  end

  describe :run! do
    it 'skips cleanup if tokens have not changed' do
      nodetool_cleanup = lambda { throw 'nodetool clenaup should not run' }
      tokens = ['1', '2', '3']
      cached_tokens = ['1', '2', '3']

      @cleaner.stub :nodetool_cleanup, nodetool_cleanup do
        @cleaner.stub :cached_tokens, cached_tokens do
          @cleaner.stub :tokens, tokens do
            @cleaner.run!
          end
        end
      end
    end

    it 'saves tokens when cleanup finishes' do
      nodetool_cleanup = OpenStruct.new(exitstatus: 0)
      token_cache = Tempfile.new('autoclean')
      tokens = ['1', '2', '3']
      cached_tokens = []

      @cleaner.stub :nodetool_cleanup, nodetool_cleanup do
        @cleaner.stub :token_cache, token_cache do
          @cleaner.stub :cached_tokens, cached_tokens do
            @cleaner.stub :tokens, tokens do
              @cleaner.run!
            end
          end
        end
      end

      JSON.parse(File.read token_cache)['tokens'].must_equal ['1', '2', '3']
    end

    it 'skips token caching if cleanup fails' do
      nodetool_cleanup = OpenStruct.new(exitstatus: 1)
      token_cache = Tempfile.new('autoclean')
      tokens = ['1', '2', '3']
      cached_tokens = []

      @cleaner.stub :nodetool_cleanup, nodetool_cleanup do
        @cleaner.stub :token_cache, token_cache do
          @cleaner.stub :cached_tokens, cached_tokens do
            @cleaner.stub :tokens, tokens do
              @cleaner.run!
            end
          end
        end
      end

      File.read(token_cache).must_be_empty
    end

    it 'tracks existing cleanup processes before launching new ones' do
      find_nodetool_cleanup = 2600
      exec_nodetool_cleanup = lambda { throw 'nodetool cleanup should not run' }
      wait_nodetool_cleanup = OpenStruct.new(exitstatus: 0)
      token_cache = Tempfile.new('autoclean')
      tokens = ['1', '2', '3']
      cached_tokens = []

      @cleaner.stub :find_nodetool_cleanup, find_nodetool_cleanup do
        @cleaner.stub :exec_nodetool_cleanup, exec_nodetool_cleanup do
          @cleaner.stub :wait_nodetool_cleanup, wait_nodetool_cleanup do
            @cleaner.stub :token_cache, token_cache do
              @cleaner.stub :cached_tokens, cached_tokens do
                @cleaner.stub :tokens, tokens do
                  @cleaner.run!
                end
              end
            end
          end
        end
      end

      JSON.parse(File.read token_cache)['tokens'].must_equal ['1', '2', '3']
    end

    it 'launches a new cleanup processes if an existing one is not found' do
      exec_nodetool_cleanup = 2600
      wait_nodetool_cleanup = OpenStruct.new(exitstatus: 0)
      token_cache = Tempfile.new('autoclean')
      tokens = ['1', '2', '3']
      cached_tokens = []

      @cleaner.stub :exec_nodetool_cleanup, exec_nodetool_cleanup do
        @cleaner.stub :wait_nodetool_cleanup, wait_nodetool_cleanup do
          @cleaner.stub :token_cache, token_cache do
            @cleaner.stub :cached_tokens, cached_tokens do
              @cleaner.stub :tokens, tokens do
                @cleaner.run!
              end
            end
          end
        end
      end

      JSON.parse(File.read token_cache)['tokens'].must_equal ['1', '2', '3']
    end

    it 'skips saving tokens if an existing cleanup processes times out' do
      find_nodetool_cleanup = 2600
      process_wait = lambda do |pid, options|
        pid.must_equal 2600
        raise Errno::ECHILD
      end
      token_cache = Tempfile.new('autoclean')
      tokens = ['1', '2', '3']
      cached_tokens = []

      Process.stub :wait2, process_wait do
        @cleaner.stub :find_nodetool_cleanup, find_nodetool_cleanup do
          @cleaner.stub :token_cache, token_cache do
            @cleaner.stub :cached_tokens, cached_tokens do
              @cleaner.stub :tokens, tokens do
                @cleaner.run!
              end
            end
          end
        end
      end

      File.read(token_cache).must_be_empty
    end
  end
end
