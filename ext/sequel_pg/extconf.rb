require 'mkmf'
$CFLAGS << " -O0 -g" if ENV['DEBUG']
$CFLAGS << " -Wall " unless RUBY_PLATFORM =~ /solaris/

if ENV["POSTGRES_INCLUDE"] || ENV["POSTGRES_LIB"] || !pkg_config('libpq')
dir_config('pg', ENV["POSTGRES_INCLUDE"] || (IO.popen("pg_config --includedir").readline.chomp rescue nil),
                 ENV["POSTGRES_LIB"]     || (IO.popen("pg_config --libdir").readline.chomp rescue nil))
end

if (have_library('pq') || have_library('libpq') || have_library('ms/libpq')) && have_header('libpq-fe.h')
  have_func 'PQsetSingleRowMode'
  have_func 'timegm'
  create_makefile("sequel_pg")
else
  puts 'Could not find PostgreSQL build environment (libraries & headers): Makefile not created'
end
