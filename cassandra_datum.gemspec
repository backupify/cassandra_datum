# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassandra_datum/version'


Gem::Specification.new do |gem|
  gem.name = "cassandra_datum"
  gem.version = CassandraDatum::VERSION
  gem.authors = ["Jason Haruska"]
  gem.email = ["jason@backupify.com"]
  gem.description = "Cassandra backed ORM"
  gem.summary = "An active record like object base that is backed by Cassandra"
  gem.homepage = "http://github.com/backupify/cassandra_datum"
  gem.license = "MIT"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}) { |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency(%q<activesupport>, [">= 2.3.5"])
  gem.add_runtime_dependency(%q<activemodel>, [">= 2.3.5"])
  gem.add_runtime_dependency(%q<activerecord>, [">= 2.3.5"])
  gem.add_runtime_dependency(%q<cassandra>, [">= 0"])
  gem.add_runtime_dependency(%q<active_attr>, [">= 0"])
  gem.add_runtime_dependency(%q<exception_helper>, [">= 0"])

  gem.add_development_dependency(%q<shoulda>, [">= 0"])
  gem.add_development_dependency(%q<factory_girl>, [">= 0"])
end

