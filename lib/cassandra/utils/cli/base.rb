require 'mixlib/shellout'

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
          Utils::Statsd.new(metric_name).to_dd(out).push!
          out
        end
      end
    end
  end
end
