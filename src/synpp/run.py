"""Run pipeline general function."""

import logging
import os
import json
import shutil
import datetime
import pickle
import errno
import stat

import networkx as nx
from networkx.readwrite.json_graph import node_link_data

from .exceptions import PipelineError
from .processing import process_stages
from .stage import ValidateContext, ExecuteContext


def rmtree(path):
    """
    Delete a folder.

    Extend shutil.rmtree, which by default refuses to delete write-protected
    files on Windows. However, we often want to delete .git directories, which
    are protected.
    """

    def handle_rmtree_error(delegate, path, exec):
        if delegate in (os.rmdir, os.remove) and exec[1].errno == errno.EACCES:
            os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            delegate(path)
        else:
            raise

    return shutil.rmtree(
        path, ignore_errors=False, onerror=handle_rmtree_error
    )


def update_json(meta, working_directory):
    """Update pipeline file."""
    if os.path.exists("%s/pipeline.json" % working_directory):
        shutil.move(
            "%s/pipeline.json" % working_directory,
            "%s/pipeline.json.bk" % working_directory,
        )

    with open("%s/pipeline.json.new" % working_directory, "w+") as f:
        json.dump(meta, f)

    shutil.move(
        "%s/pipeline.json.new" % working_directory,
        "%s/pipeline.json" % working_directory,
    )


def run(
    definitions,
    config={},
    working_directory=None,
    flowchart_path=None,
    dryrun=False,
    verbose=False,
    logger=logging.getLogger("synpp"),
    rerun_required=True,
    ensure_working_directory=False,
    externals={},
    aliases={},
):
    """Run the pipeline."""
    # 0) Construct pipeline config
    pipeline_config = {}
    if "processes" in config:
        pipeline_config["processes"] = config["processes"]
    if "progress_interval" in config:
        pipeline_config["progress_interval"] = config["progress_interval"]

    if ensure_working_directory and working_directory is None:
        working_directory = ".synpp_cache"
    if working_directory is not None:
        if not os.path.isdir(working_directory) and ensure_working_directory:
            logger.warning(
                "Working directory does not exist, it will be created: %s"
                % working_directory
            )
            os.mkdir(working_directory)

        working_directory = os.path.realpath(working_directory)

    # 1) Construct stage registry
    registry = process_stages(definitions, config, externals, aliases)

    required_hashes = [None] * len(definitions)
    for stage in registry.values():
        if "required-index" in stage:
            required_hashes[stage["required-index"]] = stage["hash"]

    logger.info("Found %d stages" % len(registry))

    # 2) Order stages
    graph = nx.DiGraph()
    flowchart = nx.MultiDiGraph()  # graph to later plot

    for hash in registry.keys():
        graph.add_node(hash)

    for stage in registry.values():
        stage_name = stage["descriptor"]

        if not flowchart.has_node(stage_name):
            flowchart.add_node(stage_name)

        for hash in stage["dependencies"]:
            graph.add_edge(hash, stage["hash"])

            dependency_name = registry.get(hash)["descriptor"]
            if not flowchart.has_edge(dependency_name, stage_name):
                flowchart.add_edge(dependency_name, stage_name)

    # Write out flowchart
    if flowchart_path is not None:
        flowchart_directory = os.path.dirname(os.path.abspath(flowchart_path))
        if not os.path.isdir(flowchart_directory):
            raise PipelineError(
                "Flowchart directory does not exist: %s" % flowchart_directory
            )

        logger.info(
            "Writing pipeline flowchart to : {}".format(flowchart_path)
        )
        with open(flowchart_path, "w") as outfile:
            json.dump(node_link_data(flowchart), outfile)

    if dryrun:
        return node_link_data(flowchart)

    for cycle in nx.cycles.simple_cycles(graph):
        cycle = [
            registry[hash]["hash"] for hash in cycle
        ]  # TODO: Make more verbose
        raise PipelineError("Found cycle: %s" % " -> ".join(cycle))

    sorted_hashes = list(nx.topological_sort(graph))

    # Check where cache is available
    cache_available = set()

    if working_directory is not None:
        for hash in sorted_hashes:
            directory_path = "%s/%s.cache" % (working_directory, hash)
            file_path = "%s/%s.p" % (working_directory, hash)

            if os.path.exists(directory_path) and os.path.exists(file_path):
                cache_available.add(hash)
                registry[hash]["ephemeral"] = False

    # Set up ephemeral stage counts
    ephemeral_counts = {}

    for stage in registry.values():
        for hash in stage["dependencies"]:
            dependency = registry[hash]

            if dependency["ephemeral"] and hash not in cache_available:
                if hash not in ephemeral_counts:
                    ephemeral_counts[hash] = 0

                ephemeral_counts[hash] += 1

    # 3) Load information about stages
    meta = {}

    if working_directory is not None:
        try:
            with open("%s/pipeline.json" % working_directory) as f:
                meta = json.load(f)
                logger.info(
                    "Found pipeline metadata in %s/pipeline.json"
                    % working_directory
                )
        except FileNotFoundError:
            logger.info(
                "Did not find pipeline metadata in %s/pipeline.json"
                % working_directory
            )

    # 4) Devalidate stages
    sorted_cached_hashes = sorted_hashes - ephemeral_counts.keys()
    stale_hashes = set()

    # 4.1) Devalidate if they are required (optional, otherwise will reload
    # from cache)
    if rerun_required:
        stale_hashes.update(required_hashes)

    # 4.2) Devalidate if not in meta
    for hash in sorted_cached_hashes:
        if hash not in meta:
            stale_hashes.add(hash)

    # 4.3) Devalidate if configuration values have changed
    # This devalidation step is obsolete since we have implicit config
    # parameters.

    # 4.4) Devalidate if module hash of a stage has changed
    for hash in sorted_cached_hashes:
        if hash in meta:
            if "module_hash" not in meta[hash]:
                stale_hashes.add(hash)  # Backwards compatibility

            else:
                previous_module_hash = meta[hash]["module_hash"]
                current_module_hash = registry[hash]["wrapper"].module_hash

                if previous_module_hash != current_module_hash:
                    stale_hashes.add(hash)

    # 4.5) Devalidate if cache is not existant
    if working_directory is not None:
        for hash in sorted_cached_hashes:
            directory_path = "%s/%s.cache" % (working_directory, hash)
            file_path = "%s/%s.p" % (working_directory, hash)

            if hash not in cache_available:
                stale_hashes.add(hash)

    # 4.6) Devalidate if parent has been updated
    for hash in sorted_cached_hashes:
        if hash not in stale_hashes and hash in meta:
            for dependency_hash, dependency_update in meta[hash][
                "dependencies"
            ].items():
                if dependency_hash not in meta:
                    stale_hashes.add(hash)
                else:
                    if meta[dependency_hash]["updated"] > dependency_update:
                        stale_hashes.add(hash)

    # 4.7) Devalidate if parents are not the same anymore
    for hash in sorted_cached_hashes:
        if hash not in stale_hashes and hash in meta:
            cached_hashes = set(meta[hash]["dependencies"].keys())
            current_hashes = set(
                registry[hash]["dependencies"]
                if "dependencies" in registry[hash]
                else []
            )

            if not cached_hashes == current_hashes:
                stale_hashes.add(hash)

    # 4.8) Manually devalidate stages
    for hash in sorted_cached_hashes:
        stage = registry[hash]
        cache_path = "%s/%s.cache" % (working_directory, hash)
        context = ValidateContext(stage["config"], cache_path)

        validation_token = stage["wrapper"].validate(context)
        existing_token = (
            meta[hash]["validation_token"]
            if hash in meta and "validation_token" in meta[hash]
            else None
        )

        if not validation_token == existing_token:
            stale_hashes.add(hash)

    # 4.9) Devalidate descendants of devalidated stages
    for hash in set(stale_hashes):
        for descendant_hash in nx.descendants(graph, hash):
            if descendant_hash not in stale_hashes:
                stale_hashes.add(descendant_hash)

    # 4.10) Devalidate ephemeral stages if necessary
    pending = set(stale_hashes)

    while len(pending) > 0:
        for dependency_hash in registry[pending.pop()]["dependencies"]:
            if registry[dependency_hash]["ephemeral"]:
                if dependency_hash not in stale_hashes:
                    pending.add(dependency_hash)

                stale_hashes.add(dependency_hash)

    logger.info("Devalidating %d stages:" % len(stale_hashes))
    for hash in stale_hashes:
        logger.info("- %s" % hash)

    # 5) Reset meta information
    for hash in stale_hashes:
        if hash in meta:
            del meta[hash]

    if working_directory is not None:
        update_json(meta, working_directory)

    logger.info("Successfully reset meta data")

    # 6) Execute stages
    results = [None] * len(definitions)
    cache = {}

    progress = 0

    # General dependency cache to avoid loading the same cache in several
    # stages in a row.
    dependency_cache = {}
    for hash in sorted_hashes:
        if hash in stale_hashes:
            logger.info("Executing stage %s ..." % hash)
            stage = registry[hash]

            # Delete useless cache
            for dependency_definition in list(dependency_cache.keys()):
                if dependency_definition not in stage["dependencies"]:
                    logger.info(
                        f"Deleting from memory {dependency_definition}"
                    )
                    del dependency_cache[dependency_definition]
                else:
                    logger.info(f"Keeping in memory {dependency_definition}")

            # Load stage dependencies and dependency infos
            stage_dependency_info = {}
            for dependency_hash in stage["dependencies"]:
                stage_dependency_info[dependency_hash] = meta[dependency_hash][
                    "info"
                ]
                if (
                    working_directory is not None
                    and dependency_hash not in dependency_cache
                ):
                    with open(
                        "%s/%s.p" % (working_directory, dependency_hash), "rb"
                    ) as f:
                        logger.info(
                            "Loading cache for %s ..." % dependency_hash
                        )
                        dependency_cache[dependency_hash] = pickle.load(f)

            # Prepare cache path
            cache_path = "%s/%s.cache" % (working_directory, hash)

            if working_directory is not None:
                if os.path.exists(cache_path):
                    rmtree(cache_path)
                os.mkdir(cache_path)

            context = ExecuteContext(
                stage["config"],
                stage["required_stages"],
                stage["aliases"],
                working_directory,
                stage["dependencies"],
                cache_path,
                pipeline_config,
                logger,
                cache,
                stage_dependency_info,
                dependency_cache,
            )
            result = stage["wrapper"].execute(context)
            validation_token = stage["wrapper"].validate(
                ValidateContext(stage["config"], cache_path)
            )

            if hash in required_hashes:
                results[required_hashes.index(hash)] = result

            if working_directory is None:
                cache[hash] = result
            else:
                with open("%s/%s.p" % (working_directory, hash), "wb+") as f:
                    logger.info("Writing cache for %s" % hash)
                    pickle.dump(result, f, protocol=5)
                dependency_cache[hash] = result

            # Update meta information
            meta[hash] = {
                "config": stage["config"],
                "updated": datetime.datetime.utcnow().timestamp(),
                "dependencies": {
                    dependency_hash: meta[dependency_hash]["updated"]
                    for dependency_hash in stage["dependencies"]
                },
                "info": context.stage_info,
                "validation_token": validation_token,
                "module_hash": stage["wrapper"].module_hash,
            }

            if working_directory is not None:
                update_json(meta, working_directory)

            # Clear cache for ephemeral stages if they are no longer needed
            if working_directory is not None:
                for dependency_hash in stage["dependencies"]:
                    if dependency_hash in ephemeral_counts:
                        ephemeral_counts[dependency_hash] -= 1

                        if ephemeral_counts[dependency_hash] == 0:
                            cache_directory_path = "%s/%s.cache" % (
                                working_directory,
                                dependency_hash,
                            )
                            cache_file_path = "%s/%s.p" % (
                                working_directory,
                                dependency_hash,
                            )

                            rmtree(cache_directory_path)
                            os.remove(cache_file_path)

                            logger.info(
                                "Removed ephemeral %s." % dependency_hash
                            )
                            del ephemeral_counts[dependency_hash]

            logger.info("Finished running %s." % hash)

            progress += 1
            logger.info(
                "Pipeline progress: %d/%d (%.2f%%)"
                % (
                    progress,
                    len(stale_hashes),
                    100 * progress / len(stale_hashes),
                )
            )

    if not rerun_required:
        # Load remaining previously cached results
        for hash in required_hashes:
            if results[required_hashes.index(hash)] is None:
                with open("%s/%s.p" % (working_directory, hash), "rb") as f:
                    logger.info("Loading cache for %s ..." % hash)
                    results[required_hashes.index(hash)] = pickle.load(f)

    if verbose:
        info = {}

        for hash in sorted(meta.keys()):
            info.update(meta[hash]["info"])

        return {
            "results": results,
            "stale": stale_hashes,
            "info": info,
            "flowchart": node_link_data(flowchart),
        }
    else:
        return results
