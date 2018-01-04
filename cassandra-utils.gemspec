# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassandra/utils/version'

Gem::Specification.new do |spec|
  spec.name          = "cassandra-utils"
  spec.version       = Cassandra::Utils::VERSION
  spec.authors       = ["Andrew Thompson"]
  spec.email         = ["Andrew_Thompson@rapid7.com"]

  spec.summary       = %q{Utility to manage Cassandra Monitoring and Management}
  spec.homepage      = "https://github.com/rapid7/cassandra-utils"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "bin"
  spec.executables   = ["cass-util"]
  spec.require_paths = ["lib"]

  spec.add_dependency "mixlib-shellout", "~> 2.2"
  spec.add_dependency "dogstatsd-ruby", "~> 1.6"
  spec.add_dependency "thor", "~> 0.19"
  spec.add_dependency "daemon_runner", "~> 0.4"
  spec.add_dependency "OptionParser", "~> 0.5.1"


  spec.add_development_dependency "bundler", "~> 1.12"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
