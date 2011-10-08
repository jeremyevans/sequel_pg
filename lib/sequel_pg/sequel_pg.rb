# Add faster versions of Dataset#map, #to_hash, #select_map, #select_order_map, and #select_hash
class Sequel::Postgres::Dataset
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

  # In the case where both arguments given, use an optimized version.
  def to_hash(key_column, value_column = nil)
    if value_column
      clone(:_sequel_pg_type=>:hash, :_sequel_pg_value=>[key_column, value_column]).fetch_rows(sql){|s| return s}
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
end
