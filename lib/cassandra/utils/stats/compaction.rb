module Cassandra
  module Utils
    module Stats
      class Compaction < Utils::CLI::Base

        def command
          'nodetool compactionstats'
        end

        def output
          compaction = stdout.lines.any? { |l| l.include?('Compaction') }
        end

        def metric_name
          'cassandra.compaction.running'
        end
      end
    end
  end
end
