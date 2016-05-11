# encoding: utf-8

require 'rubygems'
require 'bundler'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require 'rake'
require 'jeweler'

Jeweler::Tasks.new do |gem|
  gem.name = "em-sequel-async"
  gem.homepage = "http://github.com/tadman/em-sequel-async"
  gem.license = "MIT"
  gem.summary = %Q{Asynchronous Helper Methods for Sequel}
  gem.description = %Q{Implements a number of asynchronous helper methods for Sequel}
  gem.email = "tadman@postageapp.com"
  gem.authors = [ "Scott Tadman" ]
  gem.required_ruby_version = '>=1.9.3'
end

Jeweler::RubygemsDotOrgTasks.new

require 'rake/testtask'

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task default: :test
