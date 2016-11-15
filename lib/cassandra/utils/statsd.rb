require 'statsd'

module Cassandra
  module Utils
    class Statsd
      attr_reader :statsd, :metric_name, :value

      def initialize(metric_name)
        @statsd ||= ::Statsd.new('localhost', 8125)
        @metric_name = metric_name
        self
      end

      def to_dd(value)
        @value = (value == true ? 1 : 0)
        self
      end

      def push!(value = @value)
        statsd.gauge(metric_name, value)
      end
    end
  end
end
