# frozen_string_literal: true

module Syskit
    module Log
        module Reports # :nodoc:
            # Load Jupyter notebooks and generate a single notebook with the result
            #
            # The loading process interprets the notebooks as ERB templates,
            # passing "vars" as local variables
            #
            # @param [Array<Pathname>] notebook_paths
            def self.notebooks_load_and_concatenate(*notebook_paths, **vars)
                notebooks = notebook_paths.map do |path|
                    notebook_load(path, **vars)
                end
                notebooks_concatenate(notebooks)
            end

            # Generate a single notebook that is the concatenation of all the
            # given notebooks
            #
            # @param [Array<Hash>] notebooks
            # @return [Hash]
            def self.notebooks_concatenate(notebooks)
                result = notebooks.shift.dup
                result["cells"] =
                    notebooks.inject(result["cells"]) { |cells, nb| cells + nb["cells"] }
                result
            end

            # Load the notebook's JSON
            #
            # @param [Pathname] path
            # @return [Hash]
            def self.notebook_load(path, **vars)
                data = path.read
                JSON.parse(ERB.new(data).result_with_hash(vars))
            end
        end
    end
end
