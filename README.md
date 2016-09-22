# Cassandra::Utils

This is a utility for gathering data about the state of a Cassandra node and reporting metrics about that state.

The utility is composed of a collection of discrete _tasks_.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'cassandra-utils'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install cassandra-utils

## Usage

```bash
bin/cass-util stats
```

## Adding tasks

Add a file under `lib/cassandra/utils/` and add that new Class to [Cassandra::Utils::Daemon#tasks](/lib/cassandra/utils/daemon.rb).

You can subclass [Cassandra::Utils::CLI::Base](/lib/cassandra/utils/cli/base.rb) to simplify running shell commands and sending stats to Datadog.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/rapid7/cassandra-utils. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
