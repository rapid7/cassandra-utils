require 'test_helper'

describe Cassandra::Utils::Stats::Cleanup do
  before do
    @cleanup = Cassandra::Utils::Stats::Cleanup.new
  end

  it 'succeeds if cleanup is running' do
    @cleanup.run!.must_equal true
  end
end
