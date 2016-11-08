require 'daemon_runner'

module Cassandra
  module Utils
    class Daemon < ::DaemonRunner::Client

      def tasks
        [
          [auto_clean_task, 'run!'],
          [::Cassandra::Utils::Stats::Health.new, 'run!'],
          [::Cassandra::Utils::Stats::Compaction.new, 'run!'],
          [::Cassandra::Utils::Stats::Cleanup.new, 'run!']
        ]
      end

      private

      def auto_clean_task
        @auto_clean_task ||= ::Cassandra::Tasks::Autoclean.new(options)
      end
    end
  end
end
