

module Cassandra
  module Utils
    Method Status  <-----WHAT DO I DO?

# @return [String, nil]
def statusstate
  results = (nodetool_status || '').split("\n")
  results.map! { |line| line.strip }
  results.select! { |line| line.include? 'UN' }
  results.map! { |line| line.split(':')[1] }
  results.compact!
  return nil if results.size != 1
  results.first.strip.downcase
end

def nodetool_status
  @nodetool_info ||= DaemonRunner::ShellOut.new(command: 'nodetool status')
  @nodetool_info.run!
  @nodetool_info.stdout
end