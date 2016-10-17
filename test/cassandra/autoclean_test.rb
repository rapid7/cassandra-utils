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

    it 'returns nil when addresses do not exist' do
      Socket.stub :ip_address_list, lambda { return [] } do
        @cleaner.address.must_be_nil
      end
    end

    it 'returns the first private IPV4 address' do
      ipv6 = lambda do |address|
        require 'ipaddr'
        IPAddr.new(address).ipv4_mapped.to_string
      end

      addresses = lambda do
        [
          Addrinfo.tcp('127.0.0.1', 0),           # Loopback IPV4 address
          Addrinfo.tcp(ipv6.call('52.0.0.1'), 0), # Public IPV6 address
          Addrinfo.tcp('52.0.0.1', 0),            # Public IPV4 address
          Addrinfo.tcp(ipv6.call('10.0.0.2'), 0), # Private IPV6 address
          Addrinfo.tcp('10.0.0.1', 0),            # Private IPV4 address
          Addrinfo.tcp('10.0.0.2', 0),            # Private IPV4 address
        ]
      end

      Socket.stub :ip_address_list, addresses do
        @cleaner.address.must_equal '10.0.0.1'
      end
    end
  end
end
