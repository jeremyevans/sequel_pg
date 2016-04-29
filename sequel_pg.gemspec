SEQUEL_PG_GEMSPEC = Gem::Specification.new do |s|
  s.name = 'sequel_pg'
  s.version = '1.6.17'
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = false
  s.extra_rdoc_files = ["README.rdoc", "CHANGELOG", "MIT-LICENSE"]
  s.rdoc_options += ["--quiet", "--line-numbers", "--inline-source", '--title', 'sequel_pg: Faster SELECTs when using Sequel with pg', '--main', 'README.rdoc']
  s.summary = "Faster SELECTs when using Sequel with pg"
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.homepage = "http://github.com/jeremyevans/sequel_pg"
  s.required_ruby_version = ">= 1.8.7"
  s.files = %w(MIT-LICENSE CHANGELOG README.rdoc Rakefile ext/sequel_pg/extconf.rb ext/sequel_pg/sequel_pg.c lib/sequel_pg/sequel_pg.rb lib/sequel/extensions/pg_streaming.rb)
  s.license = 'MIT'
  s.extensions << 'ext/sequel_pg/extconf.rb'
  s.add_dependency("pg", [">= 0.8.0"])
  s.add_dependency("sequel", [">= 4.0.0"])
  s.description = <<END
sequel_pg overwrites the inner loop of the Sequel postgres
adapter row fetching code with a C version.  The C version
is significantly faster (2-6x) than the pure ruby version
that Sequel uses by default.

sequel_pg also offers optimized versions of some dataset
methods, as well as adds support for using PostgreSQL
streaming.
END
end
