"""Run pipeline general function."""

import logging
import os
import datetime
import pickle

import networkx as nx

from .processing import process_stages
from .stage import ValidateContext, ExecuteContext
from .pipeline import Pipeline, PipelineMetadata
from .functions import rmtree


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
    pipeline = Pipeline(logger)
    if "processes" in config:
        pipeline.set_processes(config["processes"])
    if "progress_interval" in config:
        pipeline.set_progress_interval(config["progress_interval"])

    if ensure_working_directory and working_directory is None:
        pipeline.set_working_directory(".synpp_cache")
    if working_directory is not None:
        if not os.path.isdir(working_directory) and ensure_working_directory:
            logger.warning(
                "Working directory does not exist, it will be created: %s"
                % working_directory
            )
            os.mkdir(working_directory)
        pipeline.set_working_directory(os.path.realpath(working_directory))

    # 1) Construct stage registry
    pipeline.set_registry(
        process_stages(definitions, config, externals, aliases)
    )

    required_hashes = [None] * len(definitions)
    for stage in pipeline.get_stages():
        if "required-index" in stage:
            required_hashes[stage["required-index"]] = stage["hash"]

    logger.info(f"Found {pipeline.get_stages_count()} stages")

    # 2) Order stages
    pipeline.construct_graph()

    # Write out flowchart
    if flowchart_path is not None:
        pipeline.output_flowchart(flowchart_path)

    if dryrun:
        return pipeline.get_node_link_data()

    pipeline.detect_cycles()

    sorted_hashes = pipeline.get_sorted_hashes()

    # Check where cache is available
    pipeline.check_available_cache()

    # Set up ephemeral stage counts
    pipeline.prepare_ephemeral()

    # 3) Load information about stages
    metadata = PipelineMetadata(logger)
    if working_directory is not None:
        metadata.try_load_metadata(working_directory)

    # 4) Devalidate stages
    sorted_cached_hashes = sorted_hashes - pipeline.get_ephemeral_stages()
    stale_hashes = set()

    # 4.1) Devalidate if they are required (optional, otherwise will reload
    # from cache)
    if rerun_required:
        stale_hashes.update(required_hashes)

    # 4.2) Devalidate if not in meta
    stale_hashes.update(sorted_cached_hashes - metadata.get_hashes())

    # 4.4) Devalidate if module hash of a stage has changed
    for hash in sorted_cached_hashes:
        if hash in metadata.get_hashes():
            if metadata.get_module_hash(hash) != pipeline.get_module_hash(
                hash
            ):
                stale_hashes.add(hash)

    # 4.5) Devalidate if cache is not existant
    if working_directory is not None:
        for hash in sorted_cached_hashes:
            if not pipeline.has_cache(hash):
                stale_hashes.add(hash)

    # 4.6) Devalidate if parent has been updated
    for hash in sorted_cached_hashes:
        if hash not in stale_hashes and metadata.has(hash):
            for (
                dependency_hash,
                dependency_update,
            ) in metadata.get_dependencies(hash).items():
                if not metadata.has(dependency_hash):
                    stale_hashes.add(hash)
                else:
                    if (
                        metadata.get_stage(dependency_hash)["updated"]
                        > dependency_update
                    ):
                        stale_hashes.add(hash)

    # 4.7) Devalidate if parents are not the same anymore
    for hash in sorted_cached_hashes:
        if hash not in stale_hashes and metadata.has(hash):
            cached_hashes = set(metadata.get_dependencies(hash).keys())
            current_hashes = set(
                pipeline.get_dependencies(hash)
                if "dependencies" in pipeline.get_stage(hash)
                else []
            )

            if not cached_hashes == current_hashes:
                stale_hashes.add(hash)

    # 4.8) Manually devalidate stages
    if working_directory:
        for hash in sorted_cached_hashes:
            stage = pipeline.get_stage(hash)
            cache_path = pipeline.get_stage_cache_dir(hash)
            context = ValidateContext(stage["config"], cache_path)

            if not stage["wrapper"].validate(
                context
            ) == metadata.get_validation_token(hash):
                stale_hashes.add(hash)

    # 4.9) Devalidate descendants of devalidated stages
    for hash in set(stale_hashes):
        for descendant_hash in nx.descendants(pipeline.graph, hash):
            if descendant_hash not in stale_hashes:
                stale_hashes.add(descendant_hash)

    # 4.10) Devalidate ephemeral stages if necessary
    pending = set(stale_hashes)

    while len(pending) > 0:
        for dependency_hash in pipeline.get_stage(pending.pop())[
            "dependencies"
        ]:
            if pipeline.get_stage(dependency_hash)["ephemeral"]:
                if dependency_hash not in stale_hashes:
                    pending.add(dependency_hash)

                stale_hashes.add(dependency_hash)

    logger.info("Devalidating %d stages:" % len(stale_hashes))
    for hash in stale_hashes:
        logger.info("- %s" % hash)

    # 5) Reset meta information
    metadata.reset(stale_hashes)

    if working_directory is not None:
        metadata.update_json(working_directory)

    logger.info("Successfully reset meta data")

    # 6) Execute stages
    results = [None] * len(definitions)
    cache = {}

    progress = 0

    # General dependency cache to avoid loading the same cache in several
    # stages in a row.
    for hash in sorted_hashes:
        if hash in stale_hashes:
            logger.info(f"Executing stage {hash}...")
            stage = pipeline.get_stage(hash)

            # Delete useless cache
            for dependency_definition in list(cache.keys()):
                if (
                    dependency_definition not in stage["dependencies"]
                    and working_directory is not None
                ):
                    del cache[dependency_definition]

            # Load stage dependencies and dependency infos
            stage_dependency_info = {}
            for dependency_hash in stage["dependencies"]:
                stage_dependency_info[dependency_hash] = metadata.get_info(
                    dependency_hash
                )
                if (
                    working_directory is not None
                    and dependency_hash not in cache
                ):
                    cache[dependency_hash] = pipeline.load_cache(
                        dependency_hash
                    )

            # Prepare cache path
            if working_directory is None:
                cache_path = None
            else:
                cache_path = pipeline.get_stage_cache_dir(hash)
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
                pipeline.get_config(),
                logger,
                cache,
                stage_dependency_info,
            )
            result = stage["wrapper"].execute(context)
            validation_token = stage["wrapper"].validate(
                ValidateContext(stage["config"], cache_path)
            )

            if hash in required_hashes:
                results[required_hashes.index(hash)] = result

            cache[hash] = result

            # Update meta information
            metadata.set_data(
                hash,
                stage["config"],
                datetime.datetime.utcnow().timestamp(),
                stage["dependencies"],
                context.stage_info,
                validation_token,
                stage["wrapper"].module_hash,
            )

            if working_directory is not None:
                pipeline.save_cache(hash, result)
                metadata.update_json(working_directory)
                # Clear cache for ephemeral stages if they are no longer needed
                pipeline.clear_ephemerals(stage["dependencies"])

            logger.info(f"Finished running {hash}.")

            progress += 1
            logger.info(
                f"Pipeline progress: {progress}/{len(stale_hashes)}"
                + f"({(100 * progress / len(stale_hashes)):.2f}%)"
            )

    if not rerun_required:
        # Load remaining previously cached results
        for hash in required_hashes:
            if results[required_hashes.index(hash)] is None:
                results[required_hashes.index(hash)] = pipeline.load_cache(
                    hash
                )

    if verbose:
        info = {}

        for hash in sorted(metadata.get_hashes()):
            info.update(metadata.get_info(hash))

        return {
            "results": results,
            "stale": stale_hashes,
            "info": info,
            "flowchart": pipeline.get_node_link_data(),
        }
    else:
        return results
