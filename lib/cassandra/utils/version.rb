module Cassandra
  module Utils
    VERSION = IO.read(File.expand_path('../../../../VERSION', __FILE__)) rescue '0.0.1'
  end
end
