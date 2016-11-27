source "http://rubygems.org"

if ENV.key?('PUPPET_VERSION')
  puppetversion = "= #{ENV['PUPPET_VERSION']}"
else
  puppetversion = ['>= 2.7']
end

gem "rake"
gem "puppet-lint", ">= 0.1.13"
gem "puppet", puppetversion
gem "rspec-puppet"
gem "librarian-puppet", ">= 0.9.11"
