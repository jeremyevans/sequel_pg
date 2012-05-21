unless Sequel::Postgres.respond_to?(:supports_streaming?)
  raise LoadError, "either sequel_pg not loaded, or an old version of sequel_pg loaded"
end
unless Sequel::Postgres.supports_streaming?
  raise LoadError, "streaming is not supported by the version of libpq in use"
end

# Database methods necessary to support streaming.  You should extend your
# Database object with this:
#
#   DB.extend Sequel::Postgres::Streaming
#
# Then you can call #stream on your datasets to use the streaming support:
#
#  DB[:table].stream.each{|row| ...}
module Sequel::Postgres::Streaming
  # Also extend the database's datasets to support streaming
  def self.extended(db)
    db.extend_datasets(DatasetMethods)
  end

  private

  # If streaming is requested, set a row processor while executing
  # the query.
  def _execute(conn, sql, opts={})
    if stream = opts[:stream]
      with_row_processor(conn, *stream){super}
    else
      super
    end
  end

  # Dataset methods used to implement streaming.
  module DatasetMethods
    # If streaming has been requested and the current dataset
    # can be streamed, request the database use streaming when
    # executing this query.
    def fetch_rows(sql, &block)
      if stream_results?
        execute(sql, :stream=>[self, block])
      else
        super
      end
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
      @opts[:stream] && streamable?
    end

    # Queries using cursors are not streamable, and queries that use
    # the map/select_map/to_hash/to_hash_groups optimizations are not
    # streamable, but other queries are streamable.
    def streamable?
      spgt = (o = @opts)[:_sequel_pg_type]
      (spgt.nil? || spgt == :model) && !o[:cursor]
    end
  end

  # Extend a database's datasets with this module to enable streaming
  # on all streamable queries:
  #
  #   DB.extend_datasets(Sequel::Postgres::Streaming::AllQueries)
  module AllQueries
    private

    # Always stream results if the query is streamable.
    def stream_results?
      streamable?
    end
  end
end
