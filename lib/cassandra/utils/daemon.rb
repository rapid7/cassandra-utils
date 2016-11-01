require 'daemon_runner'

module Cassandra
  module Utils
    class Daemon < ::DaemonRunner::Client

      def tasks
        [
          [::Cassandra::Utils::Stats::Health.new, 'run!'],
          [::Cassandra::Utils::Stats::Compaction.new, 'run!'],
          [::Cassandra::Utils::Stats::Cleanup.new, 'run!']
        ]
      end
    end
  end
end
