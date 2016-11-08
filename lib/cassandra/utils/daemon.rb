require 'daemon_runner'

module Cassandra
  module Utils
    class Daemon < ::DaemonRunner::Client

      def tasks
        [
          [auto_clean_task, 'run!'],
          [health_stat, 'run!'],
          [compaction_stat, 'run!'],
          [cleanup_stat, 'run!']
        ]
      end

      private

      def auto_clean_task
        @auto_clean_task ||= ::Cassandra::Tasks::Autoclean.new(options)
      end

      def health_stat
        @health_stat ||= ::Cassandra::Utils::Stats::Health.new
      end

      def compaction_stat
        @compaction_stat ||= ::Cassandra::Utils::Stats::Compaction.new
      end

      def cleanup_stat
        @cleanup_stat ||= ::Cassandra::Utils::Stats::Cleanup.new
      end
    end
  end
end
