require 'test_helper'

describe Cassandra::Utils::Stats::Health do
  before do
    @checker = Cassandra::Utils::Stats::Health.new
  end

  describe :run! do
    it 'succeeds if node is NORMAL and thrift and gossip are running' do
      @checker.stub :nodetool_netstats, 'Mode: NORMAL' do
        @checker.stub :thriftstate, 'true' do
          @checker.stub :gossipstate, 'true' do
            @checker.run!.must_equal true
          end
        end
      end
    end

    it 'fails if gossip is down' do
      @checker.stub :nodetool_netstats, 'Mode: NORMAL' do
        @checker.stub :gossipstate, 'false' do
          @checker.stub :thriftstate, 'true' do
            @checker.run!.must_equal false
          end
        end
      end
    end

    it 'fails if thrift is down' do
      @checker.stub :nodetool_netstats, 'Mode: NORMAL' do
        @checker.stub :thriftstate, 'false' do
          @checker.stub :gossipstate, 'true' do
            @checker.run!.must_equal false
          end
        end
      end
    end

    it 'skips thrift and gossip checks if node is not NORMAL' do
      @checker.stub :nodetool_netstats, 'Mode: JOINING' do
        @checker.run!.must_equal true
      end
    end
  end
end
