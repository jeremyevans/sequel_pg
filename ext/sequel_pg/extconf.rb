require 'mkmf'
$CFLAGS << " -O0 -g" if ENV['DEBUG']
$CFLAGS << " -Drb_tainted_str_new=rb_str_new -DNO_TAINT" if RUBY_VERSION >= '2.7'
$CFLAGS << " -Wall " unless RUBY_PLATFORM =~ /solaris/
$LDFLAGS += " -Wl,-U,_pg_get_pgconn -Wl,-U,_pg_get_result_enc_idx -Wl,-U,_pgresult_get -Wl,-U,_pgresult_stream_any " if RUBY_PLATFORM =~ /darwin/
dir_config('pg', ENV["POSTGRES_INCLUDE"] || (IO.popen("pg_config --includedir").readline.chomp rescue nil),
                 ENV["POSTGRES_LIB"]     || (IO.popen("pg_config --libdir").readline.chomp rescue nil))

if (have_library('pq') || have_library('libpq') || have_library('ms/libpq')) && have_header('libpq-fe.h')
  have_func 'rb_hash_new_capa'
  have_func 'PQsetSingleRowMode'
  have_func 'timegm'
  create_makefile("sequel_pg")
else
  puts 'Could not find PostgreSQL build environment (libraries & headers): Makefile not created'
end
