require 'mixlib/shellout'
require 'statsd'

module Cassandra
  module Utils
    module CLI
      class Base
        attr_reader :command, :stdout

        def cwd
          '/tmp'
        end

        def timeout
          15
        end

        def runner
          @command ||= Mixlib::ShellOut.new(command, :cwd => cwd, :timeout => timeout)
        end

        def output
          raise NotImplementedError, 'Must implement this in a subclass'
        end

        def run!
          runner
          @command.run_command
          @command.error!
          @stdout = @command.stdout
          out = output
          push_metric(out)
          out
        end

        protected

        def statsd
          @statsd ||= ::Statsd.new('localhost', 8125)
        end

        def push_metric(value)
          statsd.gauge(metric_name, value)
        end

        def to_dd(out)
          out == true ? 1 : 0
        end
      end
    end
  end
end
