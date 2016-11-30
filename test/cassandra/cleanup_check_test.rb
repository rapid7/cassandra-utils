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

describe Cassandra::Utils::Stats::Cleanup do
  before do
    @cleanup = Cassandra::Utils::Stats::Cleanup.new
  end

  it 'succeeds if cleanup is running' do
    shellout = lambda do |command, options|
      command.must_equal 'nodetool compactionstats'

      results = <<-END
      pending tasks: 1
                compaction type  keyspace  table  completed  total  unit  progress
                        Cleanup  test      test   3          100    bytes    3.14%
      Active compaction remaining time : 0h03m14s
      END

      MockShellOut.new(results)
    end

    Mixlib::ShellOut.stub :new, shellout do
      @cleanup.run!.must_equal true
    end
  end

  it 'fails if cleanup is not running' do
    shellout = lambda do |command, options|
      command.must_equal 'nodetool compactionstats'

      results = <<-END
      pending tasks: 0
      Active compaction remaining time : n/a
      END

      MockShellOut.new(results)
    end

    Mixlib::ShellOut.stub :new, shellout do
      @cleanup.run!.must_equal false
    end
  end
end
