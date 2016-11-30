require 'daemon_runner/shell_out'

module Cassandra
  module Utils
    module CLI
      class Base
        attr_reader :command, :stdout

        def cwd
          '/tmp'
        end

        def timeout
          300
        end

        def runner
          @command ||= DaemonRunner::ShellOut.new(command: command, cwd: cwd, timeout: timeout)
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
