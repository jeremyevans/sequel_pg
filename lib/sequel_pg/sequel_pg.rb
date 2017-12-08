# Add speedup for model class creation from dataset
class Sequel::Postgres::Database
  def optimize_model_load=(v)
    Sequel::Deprecation.deprecate("Database#optimize_model_load= is deprecated.  Optimized model loading is now enabled by default, and can only be disabled on a per-Dataset basis.")
    v
  end
  def optimize_model_load
    Sequel::Deprecation.deprecate("Database#optimize_model_load is deprecated.  Optimized model loading is now enabled by default, and can only be disabled on a per-Dataset basis.")
    true
  end
end

# Add faster versions of Dataset#map, #as_hash, #to_hash_groups, #select_map, #select_order_map, and #select_hash
class Sequel::Postgres::Dataset
  def optimize_model_load=(v)
    Sequel::Deprecation.deprecate("Dataset#optimize_model_load= mutation method is deprecated.  Switch to using Dataset#with_optimize_model_load, which returns a modified dataset")
    opts[:optimize_model_load] = v
  end
  def optimize_model_load
    Sequel::Deprecation.deprecate("Dataset#optimize_model_load method is deprecated.  Optimized model loading is enabled by default.")
    opts.has_key?(:optimize_model_load) ?  opts[:optimize_model_load] : true
  end

  # In the case where an argument is given, use an optimized version.
  def map(sym=nil)
    if sym
      if block_given?
        super
      else
        rows = []
        clone(:_sequel_pg_type=>:map, :_sequel_pg_value=>sym).fetch_rows(sql){|s| rows << s}
        rows
      end
    else
      super
    end
  end

  # Return a modified copy with the optimize_model_load setting changed.
  def with_optimize_model_load(v)
    clone(:optimize_model_load=>v)
  end

  # In the case where both arguments given, use an optimized version.
  def as_hash(key_column, value_column = nil, opts = Sequel::OPTS)
    if value_column && !opts[:hash]
      clone(:_sequel_pg_type=>:hash, :_sequel_pg_value=>[key_column, value_column]).fetch_rows(sql){|s| return s}
      {}
    elsif opts.empty?
      super(key_column, value_column)
    else
      super
    end
  end

  unless Sequel::Dataset.method_defined?(:as_hash)
    # Handle previous versions of Sequel that use to_hash instead of as_hash
    alias to_hash as_hash
    remove_method :as_hash
  end

  # In the case where both arguments given, use an optimized version.
  def to_hash_groups(key_column, value_column = nil, opts = Sequel::OPTS)
    if value_column && !opts[:hash]
      clone(:_sequel_pg_type=>:hash_groups, :_sequel_pg_value=>[key_column, value_column]).fetch_rows(sql){|s| return s}
      {}
    elsif opts.empty?
      super(key_column, value_column)
    else
      super
    end
  end

  if defined?(Sequel::Model::ClassMethods)
    # If model loads are being optimized and this is a model load, use the optimized
    # version.
    def each
      if optimize_model_load?
        clone(:_sequel_pg_type=>:model, :_sequel_pg_value=>row_proc).fetch_rows(sql, &Proc.new)
      else
        super
      end
    end
  end
    
  protected

  # Always use optimized version
  def _select_map_multiple(ret_cols)
    rows = []
    clone(:_sequel_pg_type=>:array).fetch_rows(sql){|s| rows << s}
    rows
  end

  # Always use optimized version
  def _select_map_single
    rows = []
    clone(:_sequel_pg_type=>:first).fetch_rows(sql){|s| rows << s}
    rows
  end

  private

  if defined?(Sequel::Model::ClassMethods)
    # The model load can only be optimized if it's for a model and it's not a graphed dataset
    # or using a cursor.
    def optimize_model_load?
      (rp = row_proc) &&
        rp.is_a?(Class) &&
        rp < Sequel::Model &&
        rp.method(:call).owner == Sequel::Model::ClassMethods &&
        opts[:optimize_model_load] != false &&
        !opts[:use_cursor] &&
        !opts[:graph]
    end
  end
end

if defined?(Sequel::Postgres::PGArray)
  # pg_array extension previously loaded

  class Sequel::Postgres::PGArray::Creator
    # Override Creator to use sequel_pg's C-based parser instead of the pure ruby parser.
    def call(string)
      Sequel::Postgres::PGArray.new(Sequel::Postgres.parse_pg_array(string, @converter), @type)
    end
  end

  # Remove the pure-ruby parser, no longer needed.
  Sequel::Postgres::PGArray.send(:remove_const, :Parser)
end
