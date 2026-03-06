gem 'minitest'
ENV['MT_NO_PLUGINS'] = '1' # Work around stupid autoloading of plugins
require 'minitest/global_expectations/autorun'

require 'sequel/core'

Sequel.extension :pg_array
db = Sequel.connect(ENV['SEQUEL_PG_SPEC_URL'] || 'postgres:///?user=sequel_test')
db.extension :pg_streaming
Sequel::Deprecation.output = false

describe 'sequel_pg' do
  if defined?(Process::CLOCK_MONOTONIC)
    def clock_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  else
    def clock_time
      Time.now
    end
  end

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
    ds = db.from{generate_series(1,10000)}.select{md5(random.function.cast_string)}

    rows_read = 0
    error_raised = false

    start_time = clock_time

    begin
      ds.stream.each do |row|
        rows_read += 1
        raise "interrupted" if rows_read >= 5
      end
    rescue RuntimeError => e
      raise unless e.message == "interrupted"
      error_raised = true
    end

    elapsed = clock_time - start_time

    error_raised.must_equal true
    rows_read.must_equal 5
    elapsed.must_be :<, 2.0

    # Verify connection is still usable
    ds.count.must_equal 10000
  end

  it "should clean up quickly when streaming a large result set is interrupted" do
    # 100,000 rows with non-trivial data
    ds = db.from{generate_series(1,100_000).as(:i, [:i])}.select{[:i, repeat(md5(random.function.cast_string), 6)]}

    rows_read = 0
    start_time = clock_time

    begin
      ds.stream.each do |row|
        rows_read += 1
        raise "stop" if rows_read >= 10
      end
    rescue RuntimeError => e
      raise unless e.message == "stop"
      error_raised = true
    end

    elapsed = clock_time - start_time

    # Without cancel, draining 99,990 remaining rows takes noticeable time.
    # With cancel, cleanup should be nearly instant.
    elapsed.must_be :<, 2.0
    error_raised.must_equal true

    # Connection must still be healthy
    ds.count.must_equal 100000

    # Can stream again successfully
    second_rows = []
    ds.limit(3).stream.each { |r| second_rows << r }
    second_rows.length.must_equal 3
  end

  it "should stream all rows correctly when not interrupted" do
    ds = db.from{generate_series(1, 500).as(:i, [:value])}

    rows = []
    ds.stream.each do |row|
      rows << row[:value]
    end

    rows.length.must_equal 500
    rows.first.must_equal 1
    rows.last.must_equal 500
  end

  it "should handle multiple streaming interruptions on the same connection" do
    ds = db.from{generate_series(1,5000)}.select{md5(random.function.cast_string)}

    # Interrupt streaming 5 times in a row
    5.times do |attempt|
      rows_read = 0
      begin
        ds.stream.each do |row|
          rows_read += 1
          raise "stop #{attempt}" if rows_read >= 3
        end
      rescue RuntimeError => e
        raise unless e.message.start_with?("stop ")
        error_raised = true
      end
      error_raised.must_equal true
    end

    # Connection should still be perfectly healthy
    ds.count.must_equal 5000

    # Full stream should still work
    all_rows = []
    ds.stream.each { |r| all_rows << r }
    all_rows.length.must_equal 5000
  end
end
