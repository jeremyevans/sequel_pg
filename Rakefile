require "rake/clean"

CLEAN.include %w'**.rbc rdoc coverage'

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

# This assumes you have sequel checked out in ../sequel, and that
# spec_postgres is setup to run Sequel's PostgreSQL specs.
desc "Run Sequel's tests with coverage"
task :spec_cov=>:compile do
  ENV['RUBYLIB'] = "#{__dir__}/lib:#{ENV['RUBYLIB']}"
  ENV['RUBYOPT'] = "-r #{__dir__}/spec/coverage_helper.rb #{ENV['RUBYOPT']}"
  ENV['SIMPLECOV_COMMAND_NAME'] = "sequel_pg"
  sh %'#{FileUtils::RUBY} -I ../sequel/lib spec/sequel_pg_spec.rb'

  ENV['RUBYOPT'] = "-I lib -r sequel -r sequel/extensions/pg_array #{ENV['RUBYOPT']}"
  ENV['SEQUEL_PG_STREAM'] = "1"
  ENV['SIMPLECOV_COMMAND_NAME'] = "sequel"
  sh %'cd ../sequel && #{FileUtils::RUBY} spec/adapter_spec.rb postgres'
end

desc "Run Sequel's tests with coverage"
task :spec_ci=>:compile do
  ENV['SEQUEL_PG_SPEC_URL'] = ENV['SEQUEL_POSTGRES_URL'] = "postgres://localhost/?user=postgres&password=postgres"
  sh %'#{FileUtils::RUBY} -I lib -I ../sequel/lib spec/sequel_pg_spec.rb'
  sh %'cd ../sequel && #{FileUtils::RUBY} -I lib -I ../sequel_pg/lib spec/adapter_spec.rb postgres'
end
