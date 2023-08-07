# frozen_string_literal: true

require "roby"
require "syskit"
require "thor"

require "syskit/log"
require "syskit/log/datastore/normalize"
require "syskit/log/datastore/import"
require "syskit/log/datastore/index_build"
require "tty-progressbar"
require "tty-prompt"
require "pocolog/cli/null_reporter"
require "pocolog/cli/tty_reporter"

module Syskit::Log
    module CLI
        # CLI entrypoint for `syskit ds` (a.k.a. syskit datastore)
        class Datastore < Thor # rubocop:disable Metrics/ClassLength
            namespace "datastore"

            class_option :silent, type: :boolean, default: false
            class_option :colors, type: :boolean, default: TTY::Color.color?
            class_option :progress, type: :boolean, default: TTY::Color.color?
            class_option :store, type: :string

            stop_on_unknown_option! :roby_log
            stop_on_unknown_option! :pocolog
            check_unknown_options! except: %I[roby_log pocolog]

            def self.exit_on_failure?
                true
            end

            no_commands do
                def create_reporter(
                    format = "",
                    progress: options[:progress],
                    colors: options[:colors],
                    silent: options[:silent],
                    **options
                )
                    if silent
                        Pocolog::CLI::NullReporter.new
                    else
                        Pocolog::CLI::TTYReporter.new(
                            format, progress: progress, colors: colors, **options
                        )
                    end
                end

                def create_pastel
                    Pastel.new(enabled: options[:colors])
                end

                def datastore_path
                    unless (path = options[:store] || ENV["SYSKIT_LOG_STORE"])
                        raise ArgumentError,
                              "you must provide a path to a datastore either "\
                              "with the --store option or through the "\
                              "SYSKIT_LOG_STORE environment variable"
                    end
                    Pathname.new(path)
                end

                def open_store
                    Syskit::Log::Datastore.new(datastore_path.realpath)
                end

                def create_store
                    Syskit::Log::Datastore.create(datastore_path)
                end

                def show_dataset_short(pastel, store, dataset, long_digest: false)
                    description = dataset.metadata_fetch_all(
                        "description", "<no description>"
                    )
                    digest = store.short_digest(dataset) unless long_digest
                    format = "% #{digest.size}s"
                    description.zip([digest]) do |a, b|
                        puts "#{pastel.bold(format % [b])} #{a}"
                    end
                end

                def show_dataset(pastel, store, dataset, long_digest: false)
                    show_dataset_short(pastel, store, dataset, long_digest: long_digest)
                    metadata = dataset.metadata
                    metadata.each.sort_by(&:first).each do |k, v|
                        next if k == "description"

                        if v.size == 1
                            puts "  #{k}: #{v.first}"
                        else
                            puts "  #{k}:"
                            v.each do |value|
                                puts "  - #{value}"
                            end
                        end
                    end
                end

                def format_date(time)
                    time.strftime("%Y-%m-%d")
                end

                def format_time(time)
                    time.strftime("%H:%M:%S.%6N %z")
                end

                def format_duration(time)
                    "%4i:%02i:%02i.%06i" % [
                        Integer(time / 3600),
                        Integer((time % 3600) / 60),
                        Integer(time % 60),
                        Integer((time * 1_000_000) % 1_000_000)
                    ]
                end

                def show_task_objects(objects, name_field_size)
                    format = "      %-#{name_field_size + 1}s %s"

                    stream_sizes = objects.map do |_, stream|
                        stream.size.to_s
                    end
                    stream_size_field_size = stream_sizes.map(&:size).max
                    stream_sizes = stream_sizes.map do |size|
                        "% #{stream_size_field_size}s" % [size]
                    end
                    objects.each_with_index do |(name, stream), i|
                        if stream.empty?
                            puts format % ["#{name}:", "empty"]
                        else
                            interval_lg = stream.interval_lg.map do |t|
                                format_date(t) + " " + format_time(t)
                            end
                            duration_lg = format_duration(stream.duration_lg)
                            puts format % [
                                "#{name}:",
                                "#{stream_sizes[i]} samples from #{interval_lg[0]} "\
                                "to #{interval_lg[1]} [#{duration_lg}]"
                            ]
                        end
                    end
                end

                def show_dataset_pocolog(dataset)
                    tasks = dataset.each_task(
                        load_models: false, skip_tasks_without_models: false
                    ).to_a
                    stream_count = dataset.each_pocolog_path.to_a.size
                    puts "  #{tasks.size} oroGen tasks in #{stream_count} streams"
                    tasks.each do |task|
                        ports = task.each_port_stream.to_a
                        properties = task.each_property_stream.to_a
                        puts "    #{task.task_name}[#{task.orogen_model_name}]: "\
                             "#{ports.size} ports and #{properties.size} properties"
                        name_field_size = (
                            ports.map { |name, _| name.size } +
                            properties.map { |name, _| name.size }
                        ).max
                        unless ports.empty?
                            puts "    Ports:"
                            show_task_objects(ports, name_field_size)
                        end
                        unless properties.empty?
                            puts "    Properties:"
                            show_task_objects(properties, name_field_size)
                        end
                    end
                end

                # @api private
                #
                # Parse a metadata option such as --set some=value some-other=value
                def parse_metadata_option(dataset, option)
                    option.each do |arg|
                        if arg.start_with?("-")
                            dataset.metadata_delete(arg[1..-1])
                            next
                        end

                        unless (match = /^([^=\-+]+)([=\-+])(.*)$/.match(arg))
                            raise ArgumentError,
                                  "metadata setters need to be specified as "\
                                  "keyOPvalue where OP is =, + or - (got #{arg})"
                        end

                        key, op, value = match.captures
                        if op == "+"
                            dataset.metadata_add(key, value)
                        elsif op == "-"
                            existing = dataset.metadata_get(key) || Set.new
                            existing.delete(value)
                            dataset.metadata_set(key, *existing)
                        else
                            dataset.metadata_set(key, value)
                        end
                    end
                end

                def import_dataset?(datastore, path, reporter:)
                    last_import_digest, last_import_time =
                        Syskit::Log::Datastore::Import.find_import_info(path)
                    already_imported =
                        last_import_digest && datastore.has?(last_import_digest)
                    return true if !already_imported || options[:force]

                    reporter.info(
                        "#{path} already seem to have been imported as "\
                        "#{last_import_digest} at #{last_import_time}. Give "\
                        "--force to import again"
                    )
                    false
                end

                def dataset_duration(dataset)
                    dataset.each_pocolog_stream.map(&:duration_lg).max || 0
                end

                def raw_dataset?(path)
                    return unless path.directory?

                    has_pocolog_files =
                        Pathname.enum_for(:glob, path + "*.0.log").any? { true }
                    has_roby_events =
                        Pathname.enum_for(:glob, path + "*-events.log").any? { true }
                    has_process_server_info_yml = (path + "info.yml").exist?

                    has_pocolog_files &&
                        (has_roby_events || has_process_server_info_yml)
                end

                def find_raw_datasets_recursively(root_path)
                    paths = []
                    root_path.find do |p|
                        is_raw_dataset = raw_dataset?(p)
                        if is_raw_dataset
                            paths << p
                            Find.prune
                        end
                    end
                    paths
                end

                def import_dataset(
                    paths, reporter, datastore, metadata,
                    include:, delete_input: false, compress: false
                )
                    datastore.in_incoming(keep: delete_input) do |core_path, cache_path|
                        importer =
                            Syskit::Log::Datastore::Import
                            .new(datastore, reporter: reporter)
                        dataset = importer.normalize_dataset(
                            paths, core_path,
                            cache_path: cache_path,
                            include: include, delete_input: delete_input,
                            compress: compress
                        )
                        metadata.each { |k, v| dataset.metadata_set(k, *v) }
                        dataset.metadata_write_to_file
                        dataset_duration = dataset_duration(dataset)
                        unless dataset_duration >= options[:min_duration]
                            reporter.info(
                                format("#{paths.join(', ')} lasts only %<seconds>.1fs, "\
                                       "ignored", seconds: dataset_duration)
                            )
                            break
                        end

                        begin
                            importer.validate_dataset_import(
                                dataset, force: options[:force]
                            )
                        rescue Syskit::Log::Datastore::Import::DatasetAlreadyExists
                            reporter.info(
                                "#{paths.join(', ')} already seem to have been imported as "\
                                "#{dataset.compute_dataset_digest}. Give "\
                                "--force to import again"
                            )
                            break
                        end

                        dataset = importer.move_dataset_to_store(dataset)
                        t = Time.now
                        paths.each do |p|
                            Syskit::Log::Datastore::Import.save_import_info(
                                p, dataset, time: t
                            )
                        end
                        dataset
                    end
                end

                def show_dataset_roby(pastel, store, dataset); end

                # Parse a timestamp given as a string
                def parse_timestamp(time)
                    return Integer(time, 10) if time =~ /^\d+$/

                    Time.parse(time).tv_sec
                end

                TIMESTAMP_APPROX_PATTERNS = [
                    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/,
                    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})/,
                    /^(\d{4})-(\d{2})-(\d{2}) (\d{2})/,
                    /^(\d{4})-(\d{2})-(\d{2})/,
                    /^(\d{4})-(\d{2})/,
                    /^(\d{4})/
                ].freeze

                TIMESTAMP_APPROX_SCALES = [
                    nil, # never used
                    365 * 24 * 3600,
                    :month, # whole month, more complicated
                    24 * 3600,
                    3600,
                    60,
                    1
                ].freeze

                # Convert a string representing an approximate timestamp into a range
                def parse_approximate_timestamp(time)
                    match = TIMESTAMP_APPROX_PATTERNS
                            .lazy.map { |r| r.match(time) }.find { |v| v }
                    unless match
                        raise ArgumentError, "invalid approximate timestamp #{time}"
                    end

                    unless match.post_match.empty?
                        # Assume the remaining is a timezone
                        tz = match.post_match.strip
                    end

                    elements = 6.times.map do |i|
                        Integer(match[i + 1], 10) if match[i + 1]
                    end
                    base = Time.new(*elements, tz).tv_sec
                    scale, = TIMESTAMP_APPROX_SCALES
                             .each_with_index.find { |_, i| !match[i + 1] }

                    if scale == :month
                        # A whole month ... more complicated
                        last_day = Date.new(elements[0], elements[1], -1).day
                        scale = last_day * 24 * 3600
                    end

                    (base..(base + scale - 1))
                end

                # @api private
                #
                # Parse a query
                #
                # @param [String] query query statements of the form VALUE,
                #   KEY=VALUE and KEY~VALUE. The `=` sign matches exactly, while
                #   `~` matches through a regular expression. Entries with no
                #   `=` and `~` signs are returned separately
                #
                # @return [([String],{String=>#===})] a list of implicit
                #   statements (without = and ~) and a list of key to a matching
                #   object.
                def parse_query(*query)
                    implicit = []
                    explicit = query.each_with_object({}) do |kv, matchers|
                        if kv =~ /=/
                            k, v = kv.split("=")
                            matchers[k] =
                                if k == "timestamp"
                                    parse_timestamp(v)
                                else
                                    v
                                end
                        elsif kv =~ /~/
                            k, v = kv.split("~")
                            matchers[k] =
                                if k == "timestamp"
                                    parse_approximate_timestamp(v)
                                else
                                    /#{v}/
                                end
                        else # assume this is a digest
                            implicit << kv
                        end
                    end
                    [implicit, explicit]
                end

                # Resolve the list of datasets that match the given query
                #
                # @param [Datastore] store the datastore whose datasets are being
                #   resolved
                # @param (see #parse_query)
                # @param (see Datastore#get)
                #
                # @return [[Datastore]] matching datastores
                def resolve_datasets(store, *query, **get_arguments)
                    return store.each_dataset(**get_arguments) if query.empty?

                    implicit, matchers = parse_query(*query)
                    if (digest = implicit.first)
                        Syskit::Log::Datastore::Dataset
                            .validate_encoded_short_digest(digest)
                        return [store.get(digest, **get_arguments)] if matchers.empty?

                        matchers["digest"] = /^#{digest}/
                    end

                    need_timestamp = matchers.key?("timestamp")

                    store.each_dataset(**get_arguments).find_all do |dataset|
                        dataset.timestamp if need_timestamp
                        all_metadata = { "digest" => [dataset.digest] }
                                       .merge(dataset.metadata)
                        all_metadata.any? do |key, values|
                            if (v_match = matchers[key])
                                values.any? { |v| v_match === v }
                            end
                        end
                    end
                end

                KNOWN_STREAM_IMPLICIT_MATCHERS =
                    %w[ports properties].freeze
                KNOWN_STREAM_EXPLICIT_MATCHERS =
                    %w[object_name task_name task_model].freeze

                # Resolve the list of streams that match the given query
                #
                # @param [[Dataset]] datasets the list of datasets whose
                #   streams are being resolved
                # @param query (see #parse_query). The available matchers are
                #   the methods of {RockTaskMatcher}
                #
                # @return [[Streams]] per-dataset list of matching streams
                def resolve_streams(datasets, *query)
                    implicit, explicit = parse_query(*query)

                    matcher = RockStreamMatcher.new
                    implicit.each do |n|
                        matcher.send(n)
                    end
                    explicit.each do |k, v|
                        matcher.send(k, v)
                    end

                    matches, empty =
                        datasets
                        .flat_map { |ds| ds.streams.find_all_streams(matcher) }
                        .partition { |ds| ds.interval_lg[0] }

                    empty = empty.sort_by(&:name)
                    matches = matches.sort_by { |a| a.interval_lg[0] }
                    empty + matches
                end

                def index_dataset( # rubocop:disable Metrics/ParameterLists
                    store, dataset,
                    reporter:, roby: true, pocolog: true, rebuild_orogen_models: true
                )
                    index_build = Syskit::Log::Datastore::IndexBuild.new(store, dataset)
                    if pocolog
                        index_build.rebuild_pocolog_indexes(
                            force: options[:force], reporter: reporter
                        )
                    end
                    if roby
                        Syskit::DRoby::V5.rebuild_orogen_models = rebuild_orogen_models
                        index_build.rebuild_roby_index(
                            force: options[:force], reporter: reporter
                        )
                    end
                end
            end

            desc "normalize PATH [--out OUTPUT]", "normalizes a data stream into a format that is suitable for the other log management commands to work"
            method_option :out, desc: "output directory (defaults to a normalized/ folder under the source folder)",
                                default: "normalized"
            method_option :override, desc: "whether existing files in the output directory should be overriden",
                                     type: :boolean, default: false

            def normalize(path)
                path = Pathname.new(path).realpath
                output_path = Pathname.new(options["out"]).expand_path(path)
                output_path.mkpath

                paths = Syskit::Log.logfiles_in_dir(path)
                bytes_total = paths.inject(0) do |total, logfile_path|
                    total + logfile_path.size
                end
                reporter = create_reporter(
                    "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)",
                    total: bytes_total
                )

                begin
                    Syskit::Log::Datastore.normalize(paths, output_path: output_path, reporter: reporter)
                ensure reporter.finish
                end
            end

            desc "import PATH [DESCRIPTION]",
                 "normalize and import a raw dataset into a syskit-pocolog datastore"
            method_option :auto, desc: "import all datasets under PATH",
                                 type: :boolean, default: false
            method_option :force, desc: "overwrite existing datasets",
                                  type: :boolean, default: false
            method_option :min_duration, desc: "skip datasets whose duration is lower "\
                                               "than this (in seconds)",
                                         type: :numeric, default: 60
            method_option :tags, desc: "tags to be added to the dataset",
                                 type: :array, default: []
            method_option :metadata, desc: "metadata values as key=value pairs",
                                     type: :array, default: []
            method_option :merge,
                          desc: "create a single dataset from the "\
                                "datasets directly under PATH",
                          type: :boolean, default: false
            method_option :delete_input,
                          desc: "delete files once they are successfully imported",
                          type: :boolean, default: false
            method_option :compress,
                          desc: "compress the resulting dataset",
                          type: :boolean, default: false
            option :rebuild_orogen_models,
                   type: :boolean, default: false,
                   desc: "use this to disable rebuilding orogen models",
                   long_desc: <<~DESC
                       Enabled by default. Disabling it will allow to load older
                       logs for which syskit ds reports mismatching types, at the
                       cost of reducing the amount of information available.
                   DESC

            steps_str = Syskit::Log::Datastore::Import::IMPORT_DEFAULT_STEPS.join(", ")
            option :include,
                   desc: "steps to perform during import. Valid steps are: #{steps_str},"\
                         " roby_no_index",
                   type: :array,
                   default:
                       Syskit::Log::Datastore::Import::IMPORT_DEFAULT_STEPS.map(&:to_s)

            def import(root_path, description = nil)
                Syskit::DRoby::V5.rebuild_orogen_models = options[:rebuild_orogen_models]

                include = options[:include].map(&:to_sym)

                root_path = Pathname.new(root_path).realpath
                path_sets =
                    if options[:auto]
                        raw_datasets = find_raw_datasets_recursively(root_path)

                        if options[:merge]
                            [raw_datasets]
                        else
                            raw_datasets.map { |p| [p] }
                        end
                    elsif options[:merge]
                        [root_path.glob("*").find_all { |p| raw_dataset?(p) }]
                    else
                        [[root_path]]
                    end

                reporter = create_reporter
                datastore = create_store

                if path_sets.empty?
                    puts "Nothing to import"
                    return
                end

                metadata = {}
                metadata["description"] = description if description
                metadata["tags"] = options[:tags]

                path_sets.each do |paths|
                    paths = paths.sort_by { |p| p.basename.to_s }
                    if paths.size == 1
                        puts "Importing #{paths.first}"
                    else
                        print "Merging #{paths.size} datasets\n  "
                        puts paths.join("\n  ")
                    end

                    already_imported = paths.any? do |p|
                        !import_dataset?(datastore, p, reporter: reporter)
                    end
                    next if already_imported

                    dataset = import_dataset(
                        paths, reporter, datastore, metadata,
                        include: include, delete_input: options[:delete_input],
                        compress: options[:compress]
                    )
                    if dataset
                        parse_metadata_option(dataset, options[:metadata])
                        dataset.metadata_write_to_file
                        paths.each do |p|
                            Syskit::Log::Datastore::Import.save_import_info(p, dataset)
                        end
                        puts dataset.digest
                    end
                end
            end

            desc "delete QUERY", "remove data related to the datasets matched by QUERY"
            option :confirm,
                   desc: "confirm which datasets will be deleted first",
                   type: :boolean, default: true
            def delete(*query)
                store = open_store
                datasets = resolve_datasets(store, *query).sort_by(&:timestamp)

                if datasets.empty?
                    puts "No datasets matching #{query.join(' ')}"
                    return
                end

                pastel = create_pastel
                prompt = TTY::Prompt.new
                if options[:confirm]
                    datasets.each do |dataset|
                        show_dataset_short(pastel, store, dataset)
                    end
                    confirmed = prompt.ask(
                        "This command will remove #{datasets.size} datasets, continue ?",
                        convert: :bool
                    )
                    return unless confirmed
                end

                datasets.each do |dataset|
                    print "Removing "
                    show_dataset_short(pastel, store, dataset)

                    store.delete(dataset.digest)
                end
            end

            desc "index [DATASETS]", "refreshes or rebuilds (with --force) the datastore indexes"
            option :force, desc: "force rebuilding even indexes that look up-to-date",
                           type: :boolean, default: false
            option(
                :only,
                desc: "rebuild only these logs (accepted values are roby, pocolog)",
                type: :array, default: %w[roby pocolog]
            )
            option :rebuild_orogen_models,
                   type: :boolean, default: false,
                   desc: "use this to disable rebuilding orogen models",
                   long_desc: <<~DESC
                       Enabled by default. Disabling it will allow to load older
                       logs for which syskit ds reports mismatching types, at the
                       cost of reducing the amount of information available.
                   DESC
            def index(*datasets)
                only_invalid_modes = options[:only] - %w[roby pocolog]
                unless only_invalid_modes.empty?
                    raise ArgumentError,
                          "invalid modes #{only_invalid_modes} for --only. "\
                          "Valid modes are 'pocolog' and 'roby'"
                end

                store = open_store
                datasets = resolve_datasets(store, *datasets)
                reporter = create_reporter
                datasets.each do |dataset|
                    reporter.title "Processing #{dataset.compute_dataset_digest}"
                    index_dataset(
                        store, dataset,
                        pocolog: options[:only].include?("pocolog"),
                        roby: options[:only].include?("roby"),
                        rebuild_orogen_models: options[:rebuild_orogen_models],
                        reporter: reporter
                    )
                end
            end

            desc "path [QUERY]", "list path to datasets"
            method_option :long_digests,
                          desc: "display digests in full, instead of shortening them",
                          type: :boolean, default: false
            def path(*query)
                store = open_store
                datasets = resolve_datasets(store, *query).sort_by(&:timestamp)
                datasets.each do |dataset|
                    digest =
                        if options[:long_digests]
                            dataset.digest
                        else
                            store.short_digest(dataset)
                        end

                    puts "#{digest} #{dataset.dataset_path}"
                end
            end

            desc "repair [QUERY]", "verify and repair the given datasets"
            option "dry_run", type: :boolean, default: false
            def repair(*query)
                store = open_store

                require "syskit/log/datastore/repair"
                resolve_datasets(store, *query, validate: false).each do |ds|
                    old_digest = ds.digest
                    new_ds = Syskit::Log::Datastore::Repair
                             .repair_dataset(store, ds, dry_run: options[:dry_run])

                    if new_ds.digest != old_digest
                        store.write_redirect(
                            old_digest,
                            to: new_ds.digest,
                            doc: "created by 'syskit ds repair'"
                        )
                    end
                end
            end

            desc "list [QUERY]", "list datasets and their information"
            method_option :digest, desc: "only show the digest and no other information (for scripting)",
                                   type: :boolean, default: false
            method_option :long_digests, desc: "display digests in full form, instead of shortening them",
                                         type: :boolean, default: false
            method_option :pocolog, desc: "show detailed information about the pocolog streams in the dataset(s)",
                                    type: :boolean, default: false
            method_option :roby, desc: "show detailed information about the Roby log in the dataset(s)",
                                 type: :boolean, default: false
            method_option :all, desc: "show all available information (implies --pocolog and --roby)",
                                aliases: "a", type: :boolean, default: false
            def list(*query)
                store = open_store
                datasets = resolve_datasets(store, *query).sort_by(&:timestamp)

                pastel = create_pastel
                datasets.each do |dataset|
                    if options[:digest]
                        if options[:long_digests]
                            puts dataset.digest
                        else
                            puts store.short_digest(dataset)
                        end
                    else
                        show_dataset(pastel, store, dataset,
                                     long_digest: options[:long_digests])
                        if options[:all] || options[:roby]
                            show_dataset_roby(pastel, store, dataset)
                        end
                        if options[:all] || options[:pocolog]
                            show_dataset_pocolog(dataset)
                        end
                    end
                end
            end

            desc "metadata [QUERY] [--set=KEY=VALUE KEY=VALUE|--get=KEY]",
                 "sets or gets metadata values for a dataset or datasets"
            long_desc <<~TXT
                Gets, sets or updates metadata values for a data set or a set of datasets.
                See the global help message for the format of QUERY

                Getting a key is for instance

                   syskit ds metadata 0bef34 --get description

                And setting one is conversely

                   syskit ds metadata 0bef34 --set id=2022-034

                Metadata values are all arrays. You can add a value to a key by using `+`
                instead of `=`, and remove a value with `-`. For instance, assuming
                id=["42", "rj"], the following line turns it into ["42", "2022-034"]

                   syskit ds metadata 0bef34 --set id+2022-034 id-rj

                The processing is sequential, so you may set a key and then add more, for
                instance, the following line will set `id` to ["2022-34", "rj"], regardless
                of its current value

                   syskit ds metadata 0bef34 --set id=2022-034 id+rj
            TXT
            method_option :set, desc: "the key=value associations to set",
                                type: :array
            method_option :get, desc: "the keys to get",
                                type: :array, lazy_default: []
            method_option(
                :long_digest,
                desc: "display digests in full form, instead of shortening them",
                type: :boolean, default: false
            )
            def metadata(*query)
                if !options[:get] && !options[:set]
                    raise ArgumentError, "provide either --get or --set"
                elsif options[:get] && options[:set]
                    raise ArgumentError, "cannot provide both --get and --set at the same time"
                end

                store = open_store
                datasets = resolve_datasets(store, *query)

                digest_to_s =
                    if options[:long_digest]
                        ->(d) { d.digest }
                    else
                        store.method(:short_digest)
                    end

                if options[:set]
                    datasets.each do |set|
                        parse_metadata_option(set, options[:set])
                        set.metadata_write_to_file
                    end
                elsif options[:get].empty?
                    datasets.sort_by(&:timestamp).each do |set|
                        metadata = set.metadata.map { |k, v| [k, v.to_a.sort.join(",")] }
                                      .sort_by(&:first)
                                      .map { |k, v| "#{k}=#{v}" }
                                      .join(" ")
                        puts "#{digest_to_s[set]} #{metadata}"
                    end
                else
                    datasets.sort_by(&:timestamp).each do |set|
                        metadata = options[:get].map do |k, _|
                            [k, set.metadata_fetch_all(k, "<unset>")]
                        end
                        metadata = metadata.map { |k, v| "#{k}=#{v.to_a.sort.join(",")}" }
                                           .join(" ")
                        puts "#{digest_to_s[set]} #{metadata}"
                    end
                end
            end

            desc "find-streams [QUERY]", "find data streams matching the given query"
            long_desc <<~DESC
                Finds data streams matching a query

                The command recognizes the following query items:

                - "ports" or "properties" restrict to streams that are resp. from a port
                  or a property
                - "object_name" matches the port/property name
                - "task_name" matches the name of the port/poperty's task
                - "task_model" matches the model of the port/property's task
                - "type" matches the typename of the port/property's task

                All matchers except ports and properties expect a string to match
                against. The KEY=STRING form matches exactly, while KEY~STRING
                interprets the string as a regular expression.

                Examples:

                Match all ports of type /base/Time with:
                syskit ds find-streams ports type=/base/Time

                Match all ports and properties of tasks that have "control" in the
                name with: syskit ds find-streams task_name~control
            DESC
            method_option :ds_filter, desc: "query to apply to the datasets",
                                      type: :array, default: []
            def find_streams(*query)
                store = open_store
                datasets = resolve_datasets(store, *options[:ds_filter])
                streams = resolve_streams(datasets.sort_by(&:timestamp), *query)

                if streams.empty?
                    puts "no streams match the query"
                    return
                end

                name_field_size = streams.map { |s| s.name.size }.max
                streams = streams.map { |s| [s.name, s] }
                show_task_objects(streams, name_field_size)
            end

            desc "pocolog DATASET DATASTREAM [roby-log arguments]",
                 "execute pocolog on a stream file from a given dataset"
            def pocolog(dataset, datastream, *args, **kw)
                store = open_store
                datasets = resolve_datasets(store, dataset)

                if datasets.empty?
                    raise ArgumentError, "no dataset matches #{dataset}"
                elsif datasets.size > 1
                    raise ArgumentError, "more than one dataset matches #{dataset}"
                end

                dataset = datasets.first
                file = dataset.pocolog_path(datastream)

                exec("pocolog",
                     "--index-dir", (dataset.cache_path + "pocolog").to_s, file.to_s,
                     "-s", datastream.gsub("::", "."), *args, **kw)
            end

            desc "roby-log MODE [options] DATASET [roby-log arguments]",
                 "execute roby-log on a the Roby log of a dataset"
            option :index,
                   type: :numeric, desc: "0-based index of the log to pick",
                   long_desc: <<~DOC
                       roby-log is able to process only one log at a time. Use "
                       --index to pick which log to process if the dataset has more than
                       one (starting at 0). Use list --roby to get details on available
                       logs, or run roby-log without --index to know how many logs there
                       actually are in a dataset.
                   DOC
            def roby_log(mode, dataset, *args)
                store = open_store
                datasets = resolve_datasets(store, dataset)

                if datasets.empty?
                    raise ArgumentError, "no dataset matches #{dataset}"
                elsif datasets.size > 1
                    raise ArgumentError, "more than one dataset matches #{dataset}"
                end

                dataset = datasets.first
                roby_logs = dataset.each_roby_log_path.to_a
                if roby_logs.empty?
                    raise ArgumentError, "no Roby logs in #{dataset.digest}"
                end

                if (index = options[:index])
                    selected_log =
                        roby_logs.find { |p| /\.#{index}\.log$/.match?(p.basename.to_s) }
                    unless selected_log
                        raise ArgumentError,
                              "no log with index #{index} in #{dataset.digest}. There "\
                              "are #{roby_logs.size} logs in this dataset"
                    end

                    roby_logs = [selected_log]
                end

                if roby_logs.size > 1
                    raise ArgumentError,
                          "#{roby_logs.size} Roby logs in #{dataset.digest}, pick one with "\
                          "--index. Logs are numbered starting at 1"
                end

                roby_log_path = roby_logs.first
                exec("roby-log", mode, roby_log_path.to_s,
                     "--index-path", dataset.roby_index_path(roby_log_path).to_s,
                     *args)
            end
        end
    end
end
