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

        EventMachine.stop_event_loop
      end.resume
    end
  end
end

require 'fiber'

module Await
  # Declares an await group where any defer operations within this block will
  # need to be completed before the await block will resume.
  def await
    if (@__await)
      @__await_stack ||= [ ]
      @__await_stack << @__await
    end

    _await = @__await = {
      :fiber => Fiber.current
    }

    yield if (block_given?)

    if (@__await_stack)
      @__await = @__await_stack.pop
    end

    while (_await.length > 1)
      Fiber.yield
    end
    
    true
  end

  # Declares a defer operation. If a block is passed in, then a trigger proc
  # is passed to the block that should be executed when the operation is
  # complete. If a fiber is passed in as an argument, then the defer will
  # be considered complete when the fiber finishes.
  def defer
    _await = @__await

    trigger = lambda { |*args|
      yield(*args) if (block_given?)
      
      _await.delete(trigger)

      unless (Fiber.current == _await[:fiber])
        _await[:fiber].resume
      end
    }

    _await[trigger] = true

    trigger
  end
  
  # Import all mixin methods here as module methods so they can be used
  # directly without requiring an import.
  extend self
end

class Test::Unit::TestCase
  include Await
end
