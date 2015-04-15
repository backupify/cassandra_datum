# encoding: utf-8

require 'bundler'
require "bundler/gem_tasks"
require 'rake'
require 'rake/testtask'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs.push('test')
  test.pattern = 'test/**/*_test.rb'
end

task :default => :test

