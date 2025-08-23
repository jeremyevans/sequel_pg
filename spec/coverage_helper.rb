require 'simplecov'

SimpleCov.start do
  enable_coverage :branch
  command_name ENV['SIMPLECOV_COMMAND_NAME']
  root File.dirname(__dir__)
  add_filter{|f| f.filename.match(%r{\A#{Regexp.escape(File.dirname(__FILE__))}/})}
  add_group('Missing'){|src| src.covered_percent < 100}
  add_group('Covered'){|src| src.covered_percent == 100}
end
