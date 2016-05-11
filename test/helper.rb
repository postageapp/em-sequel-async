require 'rubygems'
require 'bundler'

begin
  Bundler.setup(:default, :development)

rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"

  exit e.status_code
end

require 'test/unit'

$LOAD_PATH.unshift(File.expand_path(File.join('..', 'lib'), File.dirname(__FILE__)))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'em-sequel-async'

require 'mysql2'
require 'sequel'
require 'eventmachine'

Sequel.extension(:em_sequel_async)

DATABASE_DEFAULTS_PATH = File.expand_path('../.database.yml', File.dirname(__FILE__))

require 'await'

class Test::Unit::TestCase
  include Await
  
  def em
    EventMachine.run do
      Fiber.new do
        yield

        EventMachine.stop_event_loop
      end.resume
    end
  end
end

def database_defaults
  @database_defaults ||= begin
    config = if (File.exist?(DATABASE_DEFAULTS_PATH))
      require 'yaml'
      
      Hash[
        YAML.load(File.open(DATABASE_DEFAULTS_PATH)).collect do |key, value|
          [ key.to_sym, value ]
        end
      ]
    else
      { }
    end

    {
      adapter: 'mysql2',
      host: 'localhost',
      username: 'test',
      password: ''
    }.merge(config)
  end
end

def database_config
  @database_config ||= {
    default: database_defaults.merge(
      database: 'emsa_test_default'
    ),
    a: database_defaults.merge(
      database: 'emsa_test_a'
    ),
    b: database_defaults.merge(
      database: 'emsa_test_b'
    )
  }
end
