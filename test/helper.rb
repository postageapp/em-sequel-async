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

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'em-sequel-async'

require 'mysql2'
require 'sequel'
require 'eventmachine'

Sequel.extension('em-sequel-async')

class Test::Unit::TestCase
  def em
    EventMachine.run do
      Fiber.new do
        yield
      end
      
      EventMachine.stop_event_loop
    end
  end
end

def defer
  if (@defer)
    @defer_stack ||= [ ]
    @defer_stack << @defer
  end
  
  fiber = Fiber.new do
    _defer = @defer = {
      :fiber => fiber
    }

    yield

    @defer = (@defer_stack and @defer_stack.pop)

    unless (_defer.length == 1)
      Fiber.yield
    end
  end
  
  fiber.resume
end

def await
  _defer = @defer
  trigger = lambda {
    _defer.delete(trigger)
    
    if (_defer.length == 1)
      _defer[:fiber].resume
    end
  }
  
  _defer[trigger] = true
  
  yield(trigger)
end
