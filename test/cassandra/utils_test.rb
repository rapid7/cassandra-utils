require 'test_helper'

class Cassandra::UtilsTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::Cassandra::Utils::VERSION
  end
end
