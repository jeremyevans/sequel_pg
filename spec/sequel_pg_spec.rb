gem 'minitest'
ENV['MT_NO_PLUGINS'] = '1' # Work around stupid autoloading of plugins
require 'minitest/global_expectations/autorun'

require 'sequel/core'

Sequel.extension :pg_array
db = Sequel.connect(ENV['SEQUEL_PG_SPEC_URL'] || 'postgres:///?user=sequel_test')
db.extension :pg_streaming
Sequel::Deprecation.output = false

describe 'sequel_pg' do
  it "should support deprecated optimized_model_load methods" do
    db.optimize_model_load.must_equal true
    db.optimize_model_load = false
    db.optimize_model_load.must_equal true
    
    ds = db.dataset
    ds.optimize_model_load.must_equal true
    proc{ds.optimize_model_load = false}.must_raise RuntimeError
    ds.with_optimize_model_load(false).optimize_model_load.must_equal false
  end

  it "should have working Sequel::Postgres::PGArray::Creator#call" do
    Sequel::Postgres::PGArray::Creator.new('text').call('{1}').must_equal ["1"]
  end

  it "should raise for map with symbol and block" do
    proc{db.dataset.map(:x){}}.must_raise Sequel::Error
  end

  it "should support paged_each with and without streaming" do
    a = []
    db.select(Sequel.as(1, :v)).paged_each{|row| a << row}
    a.must_equal [{:v=>1}]

    a = []
    db.select(Sequel.as(1, :v)).stream.paged_each{|row| a << row}
    a.must_equal [{:v=>1}]
  end

  it "should cancel the query when streaming is interrupted" do
    db.drop_table?(:streaming_cancel_test)
    db.create_table(:streaming_cancel_test) do
      primary_key :id
      String :data
    end

    db.run("INSERT INTO streaming_cancel_test (data) SELECT md5(random()::text) FROM generate_series(1, 10000)")

    rows_read = 0
    error_raised = false

    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    begin
      db[:streaming_cancel_test].stream.each do |row|
        rows_read += 1
        raise "interrupted" if rows_read >= 5
      end
    rescue RuntimeError => e
      error_raised = true if e.message == "interrupted"
    end

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

    error_raised.must_equal true
    rows_read.must_equal 5
    elapsed.must_be :<, 2.0

    # Verify connection is still usable
    db[:streaming_cancel_test].count.must_equal 10000

    db.drop_table(:streaming_cancel_test)
  end
end
