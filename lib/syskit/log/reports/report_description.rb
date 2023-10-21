# frozen_string_literal: true

module Syskit
    module Log
        module Reports
            # Representation of a report before it gets processed
            class ReportDescription
                # Load a report template from its description
                #
                # @param [Pathname] path
                # @param vars set of variables that should be set while evaluating the
                #    report description file
                # @return [ReportTemplate]
                def self.load(path, dataset_id: nil, **vars)
                    template = new
                    template.set(:dataset_id, dataset_id)
                    vars.each { |k, v| template.set(k, v) }
                    template.load(path, dataset_id: dataset_id)
                    template
                end

                def initialize
                    @notebooks = []
                    @vars = {}
                end

                # Append a template description
                def load(path, dataset_id: @dataset_id)
                    @template_path = [path.dirname, global_notebook_path]
                    context = EvaluationContext.new(self, @vars)
                    context.dataset_select dataset_id if dataset_id
                    context.instance_eval(path.read, path.to_s, 1)
                end

                # Set a variable to be passed to the notebook templates
                def set(name, value)
                    @vars[name.to_sym] = value
                end

                Notebook = Struct.new :path, :vars

                # Append a notebook to the report
                #
                # @param [Pathname] notebook_path the notebook path. Relative paths are
                #   resolved w.r.t. this package's templates folder
                def add_notebook(notebook_path, **vars)
                    @notebooks << Notebook.new(
                        resolve_notebook_path(Pathname.new(notebook_path)),
                        vars
                    )
                end

                # Render this report to JSON
                #
                # @raise EmptyReport if this report does not have any notebooks
                def to_json(*)
                    notebooks = each_loaded_notebook.map { |_, json| json }
                    if notebooks.empty?
                        raise EmptyReport, "cannot generate a report without notebooks"
                    end

                    Reports.notebooks_concatenate(notebooks)
                end

                # Render this report to HTML
                #
                # @param [Pathname,String] output path to the generated HTML
                #
                # @raise EmptyReport if this report does not have any notebooks
                def to_html(output, log: nil)
                    json = to_json
                    redirect = { out: log.to_s, err: log.to_s } if log

                    IO.popen(
                        ["jupyter-nbconvert", "--execute", "--allow-errors", "--stdin",
                         "--output=#{output}", "--no-input"], "w", **(redirect || {})
                    ) do |io|
                        io.write JSON.dump(json)
                    end
                end

                # Enumerate the notebooks that are part of this report
                #
                # @yieldparam [Pathname] path the path to the notebook on disk
                # @yieldparam [Hash] vars the notebook variables
                def each_notebook(&block)
                    @notebooks.each(&block)
                end

                # Load the notebooks that are part of this report and yield them
                #
                # @yieldparam [Pathname] path the file path
                # @yieldparam [Hash] contents the notebook contents
                def each_loaded_notebook
                    return enum_for(__method__) unless block_given?

                    each_notebook do |nb|
                        loaded = Reports.notebook_load(nb.path, **@vars.merge(nb.vars))
                        yield nb.path, loaded
                    end
                end

                # @api private
                #
                # Resolve a notebook path against this report's search path
                #
                # The method returns the path as-is if absolute. If relative, it
                # will check the report's search path, which is first the directory
                # from which the report description file was loaded, and second the
                # global template dir (this repository's 'template' folder)
                #
                # @param [Pathname] notebook_path
                # @return [Pathname]
                # @raise ArgumentError
                def resolve_notebook_path(notebook_path)
                    if notebook_path.absolute?
                        return notebook_path if notebook_path.file?

                        raise ArgumentError, "#{notebook_path} does not exist"
                    end

                    @template_path.each do |ref_path|
                        absolute = notebook_path.expand_path(ref_path)
                        return absolute if absolute.file?
                    end

                    raise ArgumentError, "cannot find #{notebook_path}"
                end

                # Path to the templates that are within this package
                #
                # @return [Pathname]
                def global_notebook_path
                    Pathname.new(__dir__) / ".." / ".." / ".." / "templates"
                end

                # @api private
                #
                # Context object used to evaluate report description files
                class EvaluationContext < Object
                    include Syskit::Log::DSL

                    def initialize(template, vars)
                        @template = template
                        @vars = vars

                        __syskit_log_dsl_initialize
                    end

                    def respond_to_missing?(name, include_private)
                        super || (@vars.key?(name) || @template.respond_to?(name))
                    end

                    def method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper
                        if @vars.key?(name)
                            unless args.empty?
                                raise ArgumentError,
                                      "expected zero argument, got #{args.size}"
                            end
                            return @vars[name]
                        end

                        @template.send(name, *args, &block)
                    end
                end
            end
        end
    end
end
