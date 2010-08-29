require "rake"
require "rake/clean"
require 'rbconfig'

CLEAN.include %w'ext/sequel_pg/Makefile ext/sequel_pg/mkmf.log ext/sequel_pg/*.*o *.rbc rdoc'
RUBY=ENV['RUBY'] || File.join(RbConfig::CONFIG['bindir'], RbConfig::CONFIG['ruby_install_name'])

desc "Do a full cleaning"
task :distclean do
  CLEAN.include %w'tmp pkg *.gem ext/sequel_pg/1.*'
  Rake::Task[:clean].invoke
end

desc "Build the extension"
task :build=>[:clean] do
  sh %{cd ext/sequel_pg && #{RUBY} extconf.rb && make}
end

desc "Build the gem"
task :gem do
  sh %{gem build sequel_pg.gemspec}
end

if RUBY_PLATFORM !~ /mswin|mingw/ and File.directory?(File.join(File.expand_path(ENV['HOME']), '.rake-compiler'))
begin
  require "rake/extensiontask"
  ENV['RUBY_CC_VERSION'] = '1.8.6:1.9.1'
  load('sequel_pg.gemspec')
  Rake::ExtensionTask.new('sequel_pg', SEQUEL_PG_GEMSPEC) do |ext|
    ext.name = 'sequel_pg'
    ext.ext_dir = 'ext/sequel_pg' 
    ext.lib_dir = 'ext/sequel_pg' 
    ext.cross_compile = true
	  ext.cross_platform = %w[i386-mswin32 i386-mingw32]

    COMPILE_HOME               = Pathname( "~/.rake-compiler" ).expand_path
    STATIC_SOURCESDIR          = COMPILE_HOME + 'sources'
    STATIC_BUILDDIR            = COMPILE_HOME + 'builds'
    POSTGRESQL_VERSION         = ENV['POSTGRESQL_VERSION'] || '8.4.4'
    STATIC_POSTGRESQL_BUILDDIR = STATIC_BUILDDIR + "postgresql-#{POSTGRESQL_VERSION}"
    STATIC_POSTGRES_LIBDIR     = STATIC_POSTGRESQL_BUILDDIR + 'src/interfaces/libpq'
    STATIC_POSTGRES_INCDIR     = STATIC_POSTGRESQL_BUILDDIR + 'src/include'
	  # configure options only for cross compile
	  ext.cross_config_options += [
  		"--with-pg-include=#{STATIC_POSTGRES_LIBDIR}",
  		"--with-opt-include=#{STATIC_POSTGRES_INCDIR}",
  		"--with-pg-lib=#{STATIC_POSTGRES_LIBDIR}",
  		"--enable-static-build",
  	]
  end
rescue LoadError
end
end

