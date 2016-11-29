require 'test_helper'

class MockShellOut
  attr_reader :stdout
  attr_accessor :valid_exit_codes

  def initialize(stdout)
    @stdout = stdout
  end

  def run_command
  end

  def error!
  end
end

describe Cassandra::Tasks::SeedRegistry do
  describe :new do
    it 'requires a non-nil cluster name' do
      proc { Cassandra::Tasks::SeedRegistry.new(nil) }.must_raise ArgumentError
    end

    it 'requires a non-empty cluster name' do
      proc { Cassandra::Tasks::SeedRegistry.new('') }.must_raise ArgumentError
    end
  end

  describe :data_center do
    it 'returns the data center for this node' do
      shellout = lambda do |command, options|
        command.must_equal 'nodetool info'

        results = <<-END
        Token                  : (invoke with -T/--tokens to see all 256 tokens)
        ID                     : uuid
        Gossip active          : true
        Thrift active          : true
        Native Transport active: true
        Load                   : 0.0 GB
        Generation No          : 0
        Uptime (seconds)       : 0
        Heap Memory (MB)       : 0 / 0
        Off Heap Memory (MB)   : 0
        Data Center            : us-east
        Rack                   : 1d
        Exceptions             : 0
        Key Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
        Row Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
        END

        MockShellOut.new(results)
      end

      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, shellout do
        registry.data_center.must_equal 'us-east'
      end
    end
  end

  describe :rack do
    it 'returns the rack for this node' do
      shellout = lambda do |command, options|
        command.must_equal 'nodetool info'

        results = <<-END
        Token                  : (invoke with -T/--tokens to see all 256 tokens)
        ID                     : uuid
        Gossip active          : true
        Thrift active          : true
        Native Transport active: true
        Load                   : 0.0 GB
        Generation No          : 0
        Uptime (seconds)       : 0
        Heap Memory (MB)       : 0 / 0
        Off Heap Memory (MB)   : 0
        Data Center            : us-east
        Rack                   : 1d
        Exceptions             : 0
        Key Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
        Row Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
        END

        MockShellOut.new(results)
      end

      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, shellout do
        registry.rack.must_equal '1d'
      end
    end
  end
end
