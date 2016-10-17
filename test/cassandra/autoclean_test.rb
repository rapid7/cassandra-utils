require 'test_helper'

# IPV4 regex pulled from Logstash's grok patterns
# https://github.com/logstash-plugins/logstash-patterns-core/blob/v4.0.2/patterns/grok-patterns#L30
IPV4 = Regexp.new('(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])')

describe Cassandra::Utils::Autoclean do
  before do
    @cleaner = Cassandra::Utils::Autoclean.new
  end

  describe :address do
    it 'resolves IPV4 addresses' do
      @cleaner.address.must_match IPV4
    end
  end
end
