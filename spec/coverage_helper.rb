require 'simplecov'

SimpleCov.start do
  coverage :line
  coverage :branch
  merge_timeout 600
  command_name ENV['SIMPLECOV_COMMAND_NAME']
  root File.dirname(__dir__)
  cover "lib/**/*.rb"
  group('Missing'){|src| src.covered_percent < 100}
end
