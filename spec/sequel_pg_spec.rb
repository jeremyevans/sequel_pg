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

  it "should clean up quickly when streaming a large result set is interrupted" do
    db.drop_table?(:streaming_large_test)
    db.create_table(:streaming_large_test) do
      primary_key :id
      String :data, size: 200
    end

    # 100,000 rows with non-trivial data
    100.times do
      db.run("INSERT INTO streaming_large_test (data) SELECT repeat(md5(random()::text), 6) FROM generate_series(1, 1000)")
    end

    rows_read = 0
    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    begin
      db[:streaming_large_test].stream.each do |row|
        rows_read += 1
        raise "stop" if rows_read >= 10
      end
    rescue RuntimeError
      # expected
    end

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

    # Without cancel, draining 99,990 remaining rows takes noticeable time.
    # With cancel, cleanup should be nearly instant.
    elapsed.must_be :<, 2.0

    # Connection must still be healthy
    db[:streaming_large_test].count.must_equal 100000

    # Can stream again successfully
    second_rows = []
    db[:streaming_large_test].limit(3).stream.each { |r| second_rows << r }
    second_rows.length.must_equal 3

    db.drop_table(:streaming_large_test)
  end

  it "should stream all rows correctly when not interrupted" do
    db.drop_table?(:streaming_normal_test)
    db.create_table(:streaming_normal_test) do
      primary_key :id
      Integer :value
    end

    db.run("INSERT INTO streaming_normal_test (value) SELECT g FROM generate_series(1, 500) g")

    rows = []
    db[:streaming_normal_test].order(:id).stream.each do |row|
      rows << row[:value]
    end

    rows.length.must_equal 500
    rows.first.must_equal 1
    rows.last.must_equal 500

    db.drop_table(:streaming_normal_test)
  end

  it "should handle multiple streaming interruptions on the same connection" do
    db.drop_table?(:streaming_multi_test)
    db.create_table(:streaming_multi_test) do
      primary_key :id
      String :data
    end

    db.run("INSERT INTO streaming_multi_test (data) SELECT md5(random()::text) FROM generate_series(1, 5000)")

    # Interrupt streaming 5 times in a row
    5.times do |attempt|
      rows_read = 0
      begin
        db[:streaming_multi_test].stream.each do |row|
          rows_read += 1
          raise "stop #{attempt}" if rows_read >= 3
        end
      rescue RuntimeError
        # expected
      end
    end

    # Connection should still be perfectly healthy
    db[:streaming_multi_test].count.must_equal 5000

    # Full stream should still work
    all_rows = []
    db[:streaming_multi_test].stream.each { |r| all_rows << r }
    all_rows.length.must_equal 5000

    db.drop_table(:streaming_multi_test)
  end
end
