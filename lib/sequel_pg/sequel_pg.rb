# Add speedup for model class creation from dataset
class Sequel::Postgres::Database
  # Whether to optimize loads for all model datasets created from this dataset.
  # Has certain limitations, see the README for details.
  attr_accessor :optimize_model_load
end

# Add faster versions of Dataset#map, #to_hash, #select_map, #select_order_map, and #select_hash
class Sequel::Postgres::Dataset
  # Set whether to enable optimized model loading for this dataset.
  attr_writer :optimize_model_load

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

  # If this dataset has turned model loading on or off, use the default value from
  # the Database object.
  def optimize_model_load
    defined?(@optimize_model_load) ? @optimize_model_load : db.optimize_model_load
  end

  # In the case where both arguments given, use an optimized version.
  def to_hash(key_column, value_column = nil)
    if value_column
      clone(:_sequel_pg_type=>:hash, :_sequel_pg_value=>[key_column, value_column]).fetch_rows(sql){|s| return s}
    else
      super
    end
  end

  # In the case where both arguments given, use an optimized version.
  def to_hash_groups(key_column, value_column = nil)
    if value_column
      clone(:_sequel_pg_type=>:hash_groups, :_sequel_pg_value=>[key_column, value_column]).fetch_rows(sql){|s| return s}
    else
      super
    end
  end

  # If model loads are being optimized and this is a model load, use the optimized
  # version.
  def each
    if (rp = row_proc) && optimize_model_load?
      clone(:_sequel_pg_type=>:model, :_sequel_pg_value=>rp).fetch_rows(sql, &Proc.new)
    else
      super
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

  # The model load can only be optimized if it's for a model and it's not a graphed dataset
  # or using a cursor.
  def optimize_model_load?
    (rp = row_proc).is_a?(Class) && (rp < Sequel::Model) && optimize_model_load && !opts[:use_cursor] && !opts[:graph]
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
