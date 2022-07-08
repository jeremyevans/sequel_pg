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
end
