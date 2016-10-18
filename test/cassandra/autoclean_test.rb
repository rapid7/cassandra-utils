require 'test_helper'
require 'tempfile'

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

describe Cassandra::Utils::Autoclean do
  before do
    @cleaner = Cassandra::Utils::Autoclean.new
  end

  describe :address do
    it 'resolves IPV4 addresses' do
      @cleaner.address.must_match IPV4
    end

    it 'returns nil when addresses do not exist' do
      Socket.stub :ip_address_list, lambda { [] } do
        @cleaner.address.must_be_nil
      end
    end

    it 'returns the first private IPV4 address' do
      ipv6 = lambda do |address|
        require 'ipaddr'
        IPAddr.new(address).ipv4_mapped.to_string
      end

      addresses = lambda do
        [
          Addrinfo.tcp('127.0.0.1', 0),           # Loopback IPV4 address
          Addrinfo.tcp(ipv6.call('52.0.0.1'), 0), # Public IPV6 address
          Addrinfo.tcp('52.0.0.1', 0),            # Public IPV4 address
          Addrinfo.tcp(ipv6.call('10.0.0.2'), 0), # Private IPV6 address
          Addrinfo.tcp('10.0.0.1', 0),            # Private IPV4 address
          Addrinfo.tcp('10.0.0.2', 0),            # Private IPV4 address
        ]
      end

      Socket.stub :ip_address_list, addresses do
        @cleaner.address.must_equal '10.0.0.1'
      end
    end
  end

  describe :tokens do
    it 'returns no tokens without an address' do
      Socket.stub :ip_address_list, lambda { [] } do
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

      addresses = lambda do
        [Addrinfo.tcp('10.0.0.1', 0)]
      end

      Mixlib::ShellOut.stub :new, shellout do
        Socket.stub :ip_address_list, addresses do
          @cleaner.tokens.must_equal ['1', '2', '3']
        end
      end
    end
  end

  describe :save_tokens do
    it 'saves tokens as JSON to disk' do
      token_cache = lambda do
        @token_cache ||= Tempfile.new('autoclean')
      end

      tokens = lambda do
        ['6', '7', '8']
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.stub :tokens, tokens do
          @cleaner.save_tokens

          data = File.read token_cache.call
          data = JSON.parse data

          data['tokens'].must_equal ['6', '7', '8']
          data['version'].must_equal ::Cassandra::Utils::VERSION
        end
      end
    end
  end

  describe :cached_tokens do
    it 'returns no tokens if token file does not exist' do
      token_cache = lambda do
        if @token_cache.nil?
          @token_cache = Tempfile.new('autoclean')
          @token_cache.close
          @token_cache.unlink
        end
        @token_cache
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns not tokens if token file fails to parse' do
      token_cache = lambda do
        if @token_cache.nil?
          @token_cache = Tempfile.new('autoclean')
          @token_cache.write('')
          @token_cache.flush
        end
        @token_cache
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns no tokens if token file is corrupt' do
      token_cache = lambda do
        if @token_cache.nil?
          @token_cache = Tempfile.new('autoclean')
          @token_cache.write({
            :version => ::Cassandra::Utils::VERSION,
            :tokens => "these are not the tokens you're looking for"
          }.to_json)
          @token_cache.flush
        end
        @token_cache
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns no tokens if version does not match' do
      token_cache = lambda do
        if @token_cache.nil?
          @token_cache = Tempfile.new('autoclean')
          @token_cache.write({
            :version => -1,
            :tokens => ['3', '1', '2']
          }.to_json)
          @token_cache.flush
        end
        @token_cache
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal []
      end
    end

    it 'returns sorted cached tokens' do
      token_cache = lambda do
        if @token_cache.nil?
          @token_cache = Tempfile.new('autoclean')
          @token_cache.write({
            :version => ::Cassandra::Utils::VERSION,
            :tokens => ['3', '1', '2']
          }.to_json)
          @token_cache.flush
        end
        @token_cache
      end

      @cleaner.stub :token_cache, token_cache do
        @cleaner.cached_tokens.must_equal ['1', '2', '3']
      end
    end
  end
end
