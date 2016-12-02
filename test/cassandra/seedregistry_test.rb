require 'test_helper'
require 'securerandom'
require 'mock_shell_out'
require 'mock_semaphore'

describe Cassandra::Tasks::SeedRegistry do
  describe :new do
    it 'requires a non-nil cluster name' do
      proc { Cassandra::Tasks::SeedRegistry.new(nil) }.must_raise ArgumentError
    end

    it 'requires a non-empty cluster name' do
      proc { Cassandra::Tasks::SeedRegistry.new('') }.must_raise ArgumentError
    end
  end

  def nodetool_info_mock(options = {})
    options = {
      gossip: true,
      thrift: true,
      native: true,
      data_center: 'us-east',
      rack: '1d'
    }.merge(options)
    if options[:data_center]
      options[:data_center] = "Data Center : #{options[:data_center]}"
    end
    if options[:rack]
      options[:rack] = "Rack : #{options[:rack]}"
    end

    return lambda do |command, unused|
      command.must_equal 'nodetool info'

      results = <<-END
      Token                  : (invoke with -T/--tokens to see all 256 tokens)
      ID                     : uuid
      Gossip active          : #{options[:gossip]}
      Thrift active          : #{options[:thrift]}
      Native Transport active: #{options[:native]}
      Load                   : 0.0 GB
      Generation No          : 0
      Uptime (seconds)       : 0
      Heap Memory (MB)       : 0 / 0
      Off Heap Memory (MB)   : 0
      #{options[:data_center]}
      #{options[:rack]}
      Exceptions             : 0
      Key Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
      Row Cache              : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
      END

      MockShellOut.new(results)
    end
  end

  describe :data_center do
    it 'returns the data center for this node' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')
      data_center = SecureRandom.hex[0..10]

      Mixlib::ShellOut.stub :new, nodetool_info_mock(data_center: data_center) do
        registry.data_center.must_equal data_center
      end
    end

    it 'returns nil if the node has no data center' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')
      data_center = SecureRandom.hex[0..10]

      Mixlib::ShellOut.stub :new, nodetool_info_mock(data_center: nil) do
        registry.data_center.must_be_nil
      end
    end
  end

  describe :rack do
    it 'returns the rack for this node' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')
      rack = SecureRandom.hex[0..10]

      Mixlib::ShellOut.stub :new, nodetool_info_mock(rack: rack) do
        registry.rack.must_equal rack
      end
    end

    it 'returns nil if the node has no rack' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock(rack: nil) do
        registry.rack.must_be_nil
      end
    end
  end

  describe :state do
    def stub_nodetool_netstats mode
      lambda do |command, options|
        command.must_equal 'nodetool netstats'

        results = <<-END
        Mode: #{mode.upcase}
        Not sending any streams.
        Read Repair Statistics:
        Attempted: 0
        Mismatch (Blocking): 0
        Mismatch (Background): 0
        Pool Name                    Active   Pending      Completed
        Commands                        n/a         0              1
        Responses                       n/a         0              1
        END

        MockShellOut.new(results)
      end
    end

    it 'is normal when the node is NORMAL' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, stub_nodetool_netstats('NORMAL') do
        registry.state.must_equal :normal
      end
    end

    it 'is leaving when the node is LEAVING' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, stub_nodetool_netstats('LEAVING') do
        registry.state.must_equal :leaving
      end
    end

    it 'is joining when the node is JOINING' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, stub_nodetool_netstats('JOINING') do
        registry.state.must_equal :joining
      end
    end

    it 'is moving when the node is MOVING' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, stub_nodetool_netstats('MOVING') do
        registry.state.must_equal :moving
      end
    end
  end

  describe :can_seed? do
    it 'is false unless the node state is NORMAL' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock do
        [:moving, :joining, :leaving].each do |state|
          registry.stub :state, state do
            registry.can_seed?.must_equal false
          end
        end
      end
    end

    it 'is false if gossip is down' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock(gossip: false) do
        registry.stub :state, :normal do
          registry.can_seed?.must_equal false
        end
      end
    end

    it 'is false if thrift is down' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock(thrift: false) do
        registry.stub :state, :normal do
          registry.can_seed?.must_equal false
        end
      end
    end

    it 'is false if native transport is down' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock(native: false) do
        registry.stub :state, :normal do
          registry.can_seed?.must_equal false
        end
      end
    end

    it 'is true if gossip, thirft, and native transport are up and node state is NORMAL' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')

      Mixlib::ShellOut.stub :new, nodetool_info_mock do
        registry.stub :state, :normal do
          registry.can_seed?.must_equal true
        end
      end
    end
  end

  describe :run! do
    it 'healthy node gets a semaphore lock' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')
      semaphore = MockSemaphore.new

      DaemonRunner::Semaphore.stub :new, semaphore do
        registry.stub :data_center, 'data-center' do
          registry.stub :rack, 'rack' do

            # Run once. We should get a lock and spawn a renew thread.
            registry.stub :can_seed?, true do
              registry.run!
            end

            semaphore.lock_call_count.must_equal 1, "First run will lock"
            semaphore.renew_call_count.must_equal 1, "First run will spawn renew"
            semaphore.kill_call_count.must_equal 0, "First run won't kill renew"
            semaphore.locked_call_count.must_equal 0, "First run won't check locked"
            semaphore.try_release_count.must_equal 0, "First run won't release lock"

            # Run twice more. The lock should be held and no new renew thread spawned.
            registry.stub :can_seed?, true do
              registry.run!
              registry.run!
            end

            semaphore.lock_call_count.must_equal 1, "Next run won't relock"
            semaphore.renew_call_count.must_equal 1, "Next run won't rerenew"
            semaphore.kill_call_count.must_equal 0, "Next run won't kill renew"
            semaphore.locked_call_count.must_equal 0, "Next run won't check locked"
            semaphore.try_release_count.must_equal 0, "Next run won't release lock"
          end
        end
      end
    end

    it 'unhealthy node releases a semaphore lock' do
      registry = Cassandra::Tasks::SeedRegistry.new('test')
      semaphore = MockSemaphore.new

      DaemonRunner::Semaphore.stub :new, semaphore do
        registry.stub :data_center, 'data-center' do
          registry.stub :rack, 'rack' do

            # Run once. We should get a lock and spawn a renew thread.
            registry.stub :can_seed?, true do
              registry.run!
            end

            semaphore.lock_call_count.must_equal 1, "Healthy run will lock"
            semaphore.renew_call_count.must_equal 1, "Healthy run will spawn renew"
            semaphore.kill_call_count.must_equal 0, "Healthy run won't kill renew"
            semaphore.locked_call_count.must_equal 0, "Healthy run won't check locked"
            semaphore.try_release_count.must_equal 0, "Healthy run won't release lock"

            # If the node fails, we should release the lock and kill the renew thread.
            registry.stub :can_seed?, false do
              registry.run!
            end

            semaphore.lock_call_count.must_equal 1, "Unhealthy run will not relock"
            semaphore.renew_call_count.must_equal 1, "Unhealthy run will not rerenew"
            semaphore.kill_call_count.must_equal 1, "Unhealthy run will kill renew"
            semaphore.locked_call_count.must_equal 2, "Unhealthy run will check locked"
            semaphore.try_release_count.must_equal 1, "Unhealthy run will release lock"
          end
        end
      end
    end
  end
end
