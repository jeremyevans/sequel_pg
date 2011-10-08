require "rake"
require "rake/clean"

CLEAN.include %w'**.rbc rdoc'

desc "Do a full cleaning"
task :distclean do
  CLEAN.include %w'tmp pkg sequel_pg*.gem lib/*.so'
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
