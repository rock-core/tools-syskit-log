# frozen_string_literal: true

require "test_helper"
require "syskit/log/datastore/import"

module Syskit::Log
    describe TaskStreams do
        before do
            create_logfile "test.0.log" do
                create_logfile_stream "/port0",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "porta",
                                                     "rock_stream_type" => "port"]
                create_logfile_stream "/port1",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "portb",
                                                     "rock_stream_type" => "port"]
                create_logfile_stream "/property0",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "propertya",
                                                     "rock_stream_type" => "property"]
                create_logfile_stream "/property1",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "propertyb",
                                                     "rock_stream_type" => "property"]
            end
        end

        def self.task_streams_behavior(stream_class) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
            describe "#find_port_by_name" do
                it "returns nil if there are no matches" do
                    assert_nil subject.find_port_by_name("does_not_exist")
                end
                it "returns the matching port stream" do
                    object = subject.find_port_by_name("porta")
                    assert_kind_of stream_class, object
                    validate_stream_name "porta", object.name
                end

                describe "access through #method_missing" do
                    it "returns a single match if there is one" do
                        validate_stream_name "porta", subject.porta_port.name
                    end
                    it "raises NoMethodError if there are no matches" do
                        assert_raises(NoMethodError) do
                            subject.does_not_exist_port
                        end
                    end
                end
            end

            describe "#find_property_by_name" do
                it "returns nil if there are no matches" do
                    assert !subject.find_property_by_name("does_not_exist")
                end
                it "returns the matching port stream" do
                    object = subject.find_property_by_name("propertya")
                    assert_kind_of stream_class, object
                    validate_stream_name "propertya", object.name
                end

                describe "access through properties" do
                    it "returns a single match if there is one" do
                        validate_stream_name(
                            "propertya", subject.properties.propertya.name
                        )
                    end
                    it "raises NoMethodError if there are no matches" do
                        assert_raises(NoMethodError) do
                            subject.properties.does_not_exist
                        end
                    end
                end
            end

            describe "#orogen_model_name" do
                describe "no model declared at all" do
                    it "raises Unknown if none is declared in the streams" do
                        assert_raises(Unknown) do
                            subject.orogen_model_name
                        end
                    end
                    it "raises Unknown if the streams are empty" do
                        assert_raises(Unknown) do
                            TaskStreams.new([]).orogen_model_name
                        end
                    end
                end

                describe "models are declared" do
                    before do
                        subject.streams.each do |s|
                            s.metadata["rock_task_model"] = "orogen::Model"
                        end
                    end

                    it "raises Unknown if some streams do not have a declared model" do
                        subject.streams.first.metadata.delete("rock_task_model")
                        assert_raises(Unknown) do
                            subject.orogen_model_name
                        end
                    end
                    it "raises Ambiguous if the streams declare multiple models" do
                        subject.streams.first.metadata["rock_task_model"] = "orogen::AnotherModel"
                        assert_raises(Ambiguous) do
                            subject.orogen_model_name
                        end
                    end
                    it "returns the model if there is only one" do
                        assert_equal "orogen::Model", subject.orogen_model_name
                    end

                    describe "#model" do
                        it "returns the resolved model" do
                            task_m = Syskit::TaskContext.new_submodel(name: "orogen::Model")
                            flexmock(task_m.orogen_model).should_receive(:name).and_return("orogen::Model")
                            assert_equal task_m, subject.model
                        end
                        it "raises Unknown if the model cannot be resolved" do
                            assert_raises(Unknown) do
                                subject.model
                            end
                        end
                    end
                end
            end
        end

        describe "from_dir" do
            attr_reader :subject

            before do
                create_logfile "testa.0.log" do
                    create_logfile_stream "/port1_2",
                                          metadata: Hash["rock_task_name" => "task",
                                                         "rock_task_object_name" => "portb",
                                                         "rock_stream_type" => "port"]
                    create_logfile_stream "/property1_2",
                                          metadata: Hash["rock_task_name" => "task",
                                                         "rock_task_object_name" => "propertyb",
                                                         "rock_stream_type" => "property"]
                end

                streams = Streams.from_dir(logfile_pathname)
                @subject = streams.find_task_by_name("task")
                assert @subject
            end

            def compress?
                false
            end

            task_streams_behavior(LazyDataStream)

            describe "#each_port_stream" do
                it "enumerates the streams that are a task's port" do
                    ports = subject.each_port_stream
                                   .map { |name, stream| [name, stream.name] }.to_set
                    expected = Set[
                        ["porta", "/port0"],
                        ["portb", "/port1"],
                        ["portb", "/port1_2"]]
                    assert_equal expected, ports
                end
            end

            describe "#each_property_stream" do
                it "enumerates the streams that are a task's property" do
                    ports = subject.each_property_stream
                                   .map { |name, stream| [name, stream.name] }.to_set
                    expected = Set[
                        ["propertya", "/property0"],
                        ["propertyb", "/property1"],
                        ["propertyb", "/property1_2"]]
                    assert_equal expected, ports
                end
            end

            describe "#find_port_by_name" do
                it "raises Ambiguous if there are more than one port with the given name" do
                    assert_raises(Ambiguous) do
                        subject.find_port_by_name("portb")
                    end
                end

                describe "access through #method_missing" do
                    it "raises Ambiguous for multiple matches" do
                        assert_raises(Ambiguous) do
                            subject.portb_port
                        end
                    end
                end
            end

            describe "#find_property_by_name" do
                it "raises Ambiguous if there are more than one port with the given name" do
                    assert_raises(Ambiguous) do
                        subject.find_property_by_name("propertyb")
                    end
                end

                describe "access through properties" do
                    it "raises Ambiguous for multiple matches" do
                        assert_raises(Ambiguous) do
                            subject.properties.propertyb
                        end
                    end
                end
            end

            def validate_stream_name(object_name, stream_name)
                map = { "a" => "0", "b" => "1" }
                m = object_name.match(/[ab]$/)
                expected = "/#{m.pre_match}#{map[m[0]]}"
                assert_equal expected, stream_name
            end
        end

        describe "from_dataset" do
            attr_reader :subject

            before do
                _, dataset = import_logfiles

                streams = Streams.from_dataset(dataset)
                @subject = streams.find_task_by_name("task")
            end

            task_streams_behavior(LazyDataStream)

            describe "#each_port_stream" do
                it "enumerates the streams that are a task's port" do
                    ports = subject.each_port_stream
                                   .map { |name, stream| [name, stream.name] }.to_set
                    expected = Set[
                        ["porta", "task.porta"],
                        ["portb", "task.portb"]
                    ]
                    assert_equal expected, ports
                end
            end

            describe "#each_property_stream" do
                it "enumerates the streams that are a task's property" do
                    ports = subject.each_property_stream
                                   .map { |name, stream| [name, stream.name] }.to_set
                    expected = Set[
                        ["propertya", "task.propertya"],
                        ["propertyb", "task.propertyb"]
                    ]
                    assert_equal expected, ports
                end
            end

            def validate_stream_name(object_name, stream_name)
                assert_equal "task.#{object_name}", stream_name
            end
        end

        it "returns a task streams that is deployable" do
            _, dataset = import_logfiles

            streams = Streams.from_dataset(dataset)
            task_streams = streams.find_task_by_name("task")
            task_streams.streams.each do |s|
                s.metadata["rock_task_model"] = "orogen::Model"
            end
            _ruby_task_m =
                Syskit::RubyTaskContext
                .new_submodel(orogen_model_name: "orogen::Model") do
                    property "propertya", "/int"
                    property "propertyb", "/int"
                    output_port "porta", "/int"
                    output_port "portb", "/int"
                end

            task = task_streams.to_instance_requirements
                               .instanciate(plan)
            group = task_streams.to_deployment_group
            candidates = group.find_all_suitable_deployments_for(task)
            assert_equal 1, candidates.size
        end

        describe "#as_data_service" do
            before do
                _, dataset = import_logfiles

                streams = Streams.from_dataset(dataset)
                @subject = streams.find_task_by_name("task")
            end

            it "returns a service-oriented TaskStreams object" do
                srv_m = Syskit::DataService.new_submodel do
                    output_port "p", "/int32_t"
                end
                srv_streams = @subject.as_data_service(srv_m, "p" => "porta")
                assert_equal "task.porta", srv_streams.p_port.name
            end

            it "enumerates the mapped ports" do
                srv_m = Syskit::DataService.new_submodel do
                    output_port "p1", "/int32_t"
                    output_port "p2", "/int32_t"
                end
                srv_streams = @subject.as_data_service(
                    srv_m, "p2" => "porta", "p1" => "portb"
                )
                mapped = srv_streams.each_port_stream.to_a.sort_by { _1 }
                assert_equal 2, mapped.size

                port_name, stream = mapped[0]
                assert_equal "p1", port_name
                assert_equal "task.portb", stream.name

                port_name, stream = mapped[1]
                assert_equal "p2", port_name
                assert_equal "task.porta", stream.name
            end

            it "raises if a non-existent service port is mentionned" do
                srv_m = Syskit::DataService.new_submodel do
                    output_port "p", "/int32_t"
                end
                e = assert_raises(ArgumentError) do
                    @subject.as_data_service(srv_m, "does_not_exist" => "porta")
                end
                assert_match(/does_not_exist/, e.message)
            end

            it "raises if a non-existent task port is mentionned" do
                srv_m = Syskit::DataService.new_submodel do
                    output_port "p", "/int32_t"
                end
                e = assert_raises(ArgumentError) do
                    @subject.as_data_service(srv_m, "p" => "does_not_exist")
                end
                assert_match(/does_not_exist/, e.message)
            end

            it "returns a task streams that can be used as the service "\
               "it represents" do
                out_srv_m = Syskit::DataService.new_submodel do
                    output_port "p", "/int32_t"
                end
                in_srv_m = Syskit::DataService.new_submodel do
                    input_port "p", "/int32_t"
                end
                cmp_m = Syskit::Composition.new_submodel do
                    add in_srv_m, as: "in"
                    add out_srv_m, as: "out"
                    out_child.connect_to in_child
                end
                srv_streams = @subject.as_data_service(out_srv_m, "p" => "portb")

                cmp = cmp_m.use("out" => srv_streams).instanciate(plan)
                out_port = cmp.out_child.portb_port
                assert_equal [[cmp.in_child.p_port, {}]],
                             out_port.enum_for(:each_concrete_connection).to_a
            end

            it "returns a task streams that is deployable" do
                out_srv_m = Syskit::DataService.new_submodel do
                    output_port "p", "/int32_t"
                end
                srv_streams = @subject.as_data_service(out_srv_m, "p" => "portb")
                task = srv_streams.to_instance_requirements
                                  .instanciate(plan)
                                  .component
                group = srv_streams.to_deployment_group
                refute group.find_all_suitable_deployments_for(task).empty?
            end
        end
    end
end
