require 'mkmf'
$CFLAGS << " -Wall " unless RUBY_PLATFORM =~ /solaris/
dir_config('pg', ENV["POSTGRES_INCLUDE"] || (IO.popen("pg_config --includedir").readline.chomp rescue nil),
                 ENV["POSTGRES_LIB"]     || (IO.popen("pg_config --libdir").readline.chomp rescue nil))

if enable_config("static-build")
	# Link against all required libraries for static build, if they are available
	have_library('gdi32', 'CreateDC') && append_library($libs, 'gdi32')
	have_library('secur32') && append_library($libs, 'secur32')
end

if (have_library('pq') || have_library('libpq') || have_library('ms/libpq')) && have_header('libpq-fe.h')
  create_makefile("sequel_pg")
else
  puts 'Could not find PostgreSQL build environment (libraries & headers): Makefile not created'
end
