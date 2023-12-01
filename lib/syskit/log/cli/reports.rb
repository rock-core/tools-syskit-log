# frozen_string_literal: true

require "syskit/log/reports"

module Syskit
    module Log
        module CLI
            # Subcommand that allow to generate HTML reports from datasets using
            # Jupyter notebooks
            class Reports < Thor
                no_commands do # rubocop:disable Metrics/BlockLength
                    # Generate an output name from the dataset digest
                    def default_output_name(dataset)
                        if dataset.respond_to?(:to_str)
                            dataset = Syskit::Log::Datastore.default.get(dataset)
                        end

                        description =
                            dataset.metadata_fetch("description", "No description")
                        date = dataset.timestamp.strftime("%Y-%m-%d")
                        "#{date} - #{description} (#{dataset.digest[0, 10]})"
                    end

                    def parse_query(vars)
                        query = {}
                        vars.each do |str|
                            key, op, value = match_single_query(str)

                            value = Regexp.new(value) if op == "~"

                            (query[key] ||= []) << value
                        end
                        query
                    end

                    def match_single_query(str)
                        unless (match = /^([^~=]+)([~=])(.*)$/.match(str))
                            raise ArgumentError,
                                  "metadata entries must be given as key=value or "\
                                  "key~value"
                        end

                        [match[1], match[2], match[3]]
                    end

                    def html_write_metadata(output, **metadata)
                        output.write JSON.generate(metadata)
                    end

                    def auto_html_processed?(output)
                        json = output.sub_ext(".json")
                        return false unless json.file?

                        json = JSON.parse(json.read)
                        !json.key?("error")
                    end

                    def auto_html_generate(template, dataset, output)
                        name = "#{default_output_name(dataset)}.html"
                        puts "Processing of #{dataset.digest}: #{name}"

                        html(template, dataset.digest, output.sub_ext(".html"),
                             log: output.sub_ext(".log"))
                        name
                    end

                    def auto_html_save_result(dataset, output, name)
                        output.sub_ext(".json").write(
                            JSON.generate({ digest: dataset.digest, name: name })
                        )
                    end

                    def auto_html_save_error(dataset, output, name, error)
                        output.sub_ext(".json").write(
                            JSON.generate(
                                digest: dataset.digest,
                                name: name,
                                error: { message: error.message,
                                         backtrace: error.backtrace }
                            )
                        )
                    end

                    def auto_html_dataset(template, dataset, output_dir)
                        output = output_dir / dataset.digest
                        return if !options[:force] && auto_html_processed?(output)

                        name = auto_html_generate(template, dataset, output)
                        auto_html_save_result(dataset, output, name)
                    rescue StandardError => e
                        puts "  Failed: #{e.message}"
                        puts "    #{e.backtrace.join("\n    ")}"
                        auto_html_save_error(dataset, output, name, e)
                    end

                    def render_single_notebook(output, path, contents)
                        output_path = output / path.basename

                        contents.each_with_index do |(_, c), i|
                            final_output =
                                if contents.size > 1
                                    output_path.sub_ext(".#{i}#{output_path.extname}")
                                else
                                    output_path
                                end

                            final_output.write(JSON.dump(c))
                        end
                    end
                end

                desc "auto-html TEMPLATE OUTPUT QUERY",
                     "render this template to HTML for every dataset "\
                     "that has not been generated yet"
                option :force, type: :boolean, default: false
                def auto_html(template, output_dir, *query)
                    query = parse_query(query)

                    output_dir = Pathname.new(output_dir)
                    output_dir.mkpath
                    datastore = Syskit::Log::Datastore.default
                    datastore.find_all(query).each do |dataset|
                        auto_html_dataset(template, dataset, output_dir)
                    end
                end

                desc "html TEMPLATE DATASET [OUTPUT]",
                     "render this template to HTML using data from the given dataset"
                option :timeout,
                       type: :numeric, default: 600,
                       desc: "execution timeout in seconds for each cell"
                def html(template, dataset_digest, output = nil, log: nil)
                    description =
                        Syskit::Log::Reports::ReportDescription
                        .load(Pathname.new(template), dataset_id: dataset_digest)

                    output = Pathname(output) if output
                    if !output || output.directory?
                        output =
                            (output || Pathname.pwd) /
                            "#{default_output_name(dataset_digest)}.html"
                    end
                    description.to_html(
                        Pathname.new(output), log: log, timeout: options[:timeout]
                    )
                end

                desc "render-notebooks REPORT DATASET [OUTPUT]",
                     "interpret each notebook from the REPORT report "\
                     "and save them in the OUTPUT directory"
                def render_notebooks(report, dataset_digest, output = nil)
                    description = Syskit::Log::Reports::ReportDescription
                                  .load(Pathname.new(report), dataset_id: dataset_digest)

                    output = Pathname.new(output) if output
                    if !output || output.directory?
                        output_dir = output || Pathname.pwd
                        output = output_dir / default_output_name(dataset_digest)
                    end

                    output.mkpath
                    notebooks = description.each_loaded_notebook.group_by do |path, _|
                        path
                    end

                    notebooks.each do |path, contents|
                        render_single_notebook(output, path, contents)
                    end
                end
            end
        end
    end
end
