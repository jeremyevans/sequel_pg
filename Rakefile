require "rake"
require "rake/clean"
require 'rbconfig'

CLEAN.include %w'**.rbc rdoc'
RUBY=ENV['RUBY'] || File.join(RbConfig::CONFIG['bindir'], RbConfig::CONFIG['ruby_install_name'])

desc "Do a full cleaning"
task :distclean do
  CLEAN.include %w'tmp pkg sequel_pg*.gem lib'
  Rake::Task[:clean].invoke
end

desc "Build the gem"
task :gem do
  sh %{gem build sequel_pg.gemspec}
end

begin
  require 'rake/extensiontask'
  Rake::ExtensionTask.new('sequel_pg')
rescue LoadError
end
