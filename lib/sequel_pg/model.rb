class Sequel::Postgres::Dataset
  # If model loads are being optimized and this is a model load, use the optimized
  # version.
  def each(&block)
    rp = row_proc
    return super unless allow_sequel_pg_optimization? && optimize_model_load?(rp)
    clone(:_sequel_pg_type=>:model, :_sequel_pg_value=>rp).fetch_rows(sql, &block)
  end

  # Avoid duplicate method warning
  alias with_sql_all with_sql_all

  # Always use optimized version
  def with_sql_all(sql, &block)
    rp = row_proc
    return super unless allow_sequel_pg_optimization?

    if optimize_model_load?(rp)
      clone(:_sequel_pg_type=>:all_model, :_sequel_pg_value=>row_proc).fetch_rows(sql) do |array|
        post_load(array)
        array.each(&block) if block
        return array
      end
      []
    else 
      clone(:_sequel_pg_type=>:all).fetch_rows(sql) do |array|
        if rp = row_proc
          array.map!{|h| rp.call(h)}
        end
        post_load(array)
        array.each(&block) if block
        return array
      end
      []
    end
  end

  private

  # The model load can only be optimized if it's for a model and it's not a graphed dataset
  # or using a cursor.
  def optimize_model_load?(rp)
    rp.is_a?(Class) &&
      rp < Sequel::Model &&
      rp.method(:call).owner == Sequel::Model::ClassMethods &&
      opts[:optimize_model_load] != false
  end
end
