unless Sequel::Postgres.respond_to?(:supports_streaming?)
  raise LoadError, "either sequel_pg not loaded, or an old version of sequel_pg loaded"
end
unless Sequel::Postgres.supports_streaming?
  raise LoadError, "streaming is not supported by the version of libpq in use"
end

# Database methods necessary to support streaming.  You should load this extension
# into your database object:
#
#   DB.extension(:pg_streaming)
#
# Then you can call #stream on your datasets to use the streaming support:
#
#   DB[:table].stream.each{|row| ...}
#
# Or change a set so that all dataset calls use streaming:
#
#   DB.stream_all_queries = true
module Sequel::Postgres::Streaming
  attr_accessor :stream_all_queries

  # Also extend the database's datasets to support streaming.
  # This extension requires modifying connections, so disconnect
  # so that new connections will get the methods.
  def self.extended(db)
    db.extend_datasets(DatasetMethods)
    db.stream_all_queries = false
    db.disconnect
  end

  # Make sure all new connections have the appropriate methods added.
  def connect(server)
    conn = super
    conn.extend(AdapterMethods)
    conn
  end

  private

  # If streaming is requested, and a prepared statement is not
  # used, tell the connection to use single row mode for the query.
  def _execute(conn, sql, opts={}, &block)
    if opts[:stream] && !sql.is_a?(Symbol) 
      conn.single_row_mode = true
    end
    super
  end

  # If streaming is requested, send the prepared statement instead
  # of executing it and blocking.
  def _execute_prepared_statement(conn, ps_name, args, opts)
    if opts[:stream]
      conn.send_prepared_statement(ps_name, args)
    else
      super
    end
  end

  module AdapterMethods
    # Whether the next query on this connection should use
    # single_row_mode.
    attr_accessor :single_row_mode

    # Send the prepared statement on this connection using
    # single row mode.
    def send_prepared_statement(ps_name, args)
      send_query_prepared(ps_name, args)
      set_single_row_mode
      block
      self
    end

    private

    # If using single row mode, send the query instead of executing it.
    def execute_query(sql, args)
      if @single_row_mode
        @single_row_mode = false
        @db.log_yield(sql, args){args ? send_query(sql, args) : send_query(sql)}
        set_single_row_mode
        block
        self
      else
        super
      end
    end
  end

  # Dataset methods used to implement streaming.
  module DatasetMethods
    # If streaming has been requested and the current dataset
    # can be streamed, request the database use streaming when
    # executing this query, and use yield_each_row to process
    # the separate PGresult for each row in the connection.
    def fetch_rows(sql)
      if stream_results?
        execute(sql, :stream=>true) do |conn|
          yield_each_row(conn){|h| yield h}
        end
      else
        super
      end
    end

    # Use streaming to implement paging.
    def paged_each(opts=Sequel::OPTS, &block)
      stream.each(&block)
    end

    # Return a clone of the dataset that will use streaming to load
    # rows.
    def stream
      clone(:stream=>true)
    end

    private

    # Only stream results if streaming has been specifically requested
    # and the query is streamable.
    def stream_results?
      (@opts[:stream] || db.stream_all_queries) && streamable?
    end

    # Queries using cursors are not streamable, and queries that use
    # the map/select_map/to_hash/to_hash_groups optimizations are not
    # streamable, but other queries are streamable.
    def streamable?
      spgt = (o = @opts)[:_sequel_pg_type]
      (spgt.nil? || spgt == :model) && !o[:cursor]
    end
  end
end

Sequel::Database.register_extension(:pg_streaming, Sequel::Postgres::Streaming)
