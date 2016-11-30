require 'test_helper'
require 'mock_shell_out'

describe Cassandra::Utils::Stats::Compaction do
  before do
    @compactor = Cassandra::Utils::Stats::Compaction.new
  end

  it 'succeeds if compaction is running' do
    shellout = lambda do |command, options|
      command.must_equal 'nodetool compactionstats'

      results = <<-END
      pending tasks: 1
                compaction type  keyspace  table  completed  total  unit  progress
                      Compaction test      test   3          100    bytes    3.14%
      Active compaction remaining time : 0h03m14s
      END

      MockShellOut.new(results)
    end

    Mixlib::ShellOut.stub :new, shellout do
      @compactor.run!.must_equal true
    end
  end

  it 'fails if compaction is not running' do
    shellout = lambda do |command, options|
      command.must_equal 'nodetool compactionstats'

      results = <<-END
      pending tasks: 0
      Active compaction remaining time : n/a
      END

      MockShellOut.new(results)
    end

    Mixlib::ShellOut.stub :new, shellout do
      @compactor.run!.must_equal false
    end
  end
end
