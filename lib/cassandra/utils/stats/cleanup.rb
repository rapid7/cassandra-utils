module Cassandra
  module Utils
    module Stats
      class Cleanup < Utils::CLI::Base

        def command
          'nodetool compactionstats'
        end

        def output
          cleanup = stdout.lines.any? { |l| l.include?('Cleanup') }
        end

        def metric_name
          'cassandra.cleanup.running'
        end

        def task_id
          ['cleanup', 'nodetool']
        end
      end
    end
  end
end
