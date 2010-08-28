require "rake"
require "rake/clean"
require 'rbconfig'

CLEAN.include %w'ext/sequel_pg/Makefile ext/sequel_pg/*.*o rdoc'
RUBY=ENV['RUBY'] || File.join(RbConfig::CONFIG['bindir'], RbConfig::CONFIG['ruby_install_name'])

desc "Build the extension"
task :build=>[:clean] do
  sh %{cd ext/sequel_pg && #{RUBY} extconf.rb && make}
end

desc "Build the gem"
task :gem do
  sh %{gem build sequel_pg.gemspec}
end

