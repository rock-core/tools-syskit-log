# frozen_string_literal: true

require "test_helper"

require "syskit/log"
require "syskit/log/datastore/import"

module Syskit
    module Log
        describe NormalizeConfiguration do
            describe "updating stream metadata" do
                before do
                    @config = NormalizeConfiguration.from_hash(
                        {
                            "streams" => [{
                                "match" => { "type" => "typename" },
                                "metadata" => [{
                                    "op" => "set", "key" => "somek",
                                    "value" => "somev"
                                }]
                            }]
                        }
                    )
                end

                it "overrides existing keys" do
                    update = @config.stream_config_for_type("typename")
                    updated = update.metadata_update({ "somek" => "otherv" })
                    assert_equal({ "somek" => "somev" }, updated)
                end

                it "sets new keys" do
                    update = @config.stream_config_for_type("typename")
                    updated = update.metadata_update({})
                    assert_equal({ "somek" => "somev" }, updated)
                end

                it "rejects an invalid configuration" do
                    assert_raises(JSON::Schema::ValidationError) do
                        NormalizeConfiguration.from_hash(
                            {
                                "streams" => [{
                                    "match" => { "invalid" => "typename" },
                                    "metadata" => [{
                                        "op" => "invalid",
                                        "invalid" => "somek",
                                        "value" => "somev"
                                    }]
                                }]
                            }
                        )
                    end
                end
            end
        end
    end
end
