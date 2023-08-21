# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new("test:lib:uncompressed") do |t|
    t.libs << "test"
    t.libs << "lib"
    t.test_files = FileList["test/**/*_test.rb"]
    t.warning = false
end
Rake::TestTask.new("test:lib:compressed") do |t|
    t.libs << "test"
    t.libs << "lib"
    t.test_files = FileList["test/**/*_test.rb", "test/test_compressed.rb"]
    t.warning = false
end
task "test" => "test:lib:compressed"
task "test" => "test:lib:uncompressed"

task :default

RUBOCOP_REQUIRED = (ENV["RUBOCOP"] == "1")
USE_RUBOCOP = (ENV["RUBOCOP"] != "0")

if USE_RUBOCOP
    begin
        require "rubocop/rake_task"
        RuboCop::RakeTask.new
        task "test" => "rubocop"
    rescue LoadError
        raise if RUBOCOP_REQUIRED
    end
end
