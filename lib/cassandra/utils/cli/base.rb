require 'daemon_runner'
require 'statsd'

module Cassandra
  module Utils
    module CLI
      class Base < ::DaemonRunner::ShellOut

        def output
          raise NotImplementedError, 'Must implement this in a subclass'
        end

        def run!
          super
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
