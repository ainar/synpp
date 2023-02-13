"""Run pipeline general function."""

import logging
import os

import networkx as nx

from .processing import process_stages
from .stage import ValidateContext
from .pipeline import Pipeline
from .pipeline_metadata import PipelineMetadata


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
    pipeline.set_metadata(metadata)

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
    for stage_hash in sorted_cached_hashes:
        if stage_hash in metadata.get_hashes():
            if metadata.get_module_hash(stage_hash) != pipeline.get_module_hash(
                stage_hash
            ):
                stale_hashes.add(stage_hash)

    # 4.5) Devalidate if cache is not existant
    if working_directory is not None:
        for stage_hash in sorted_cached_hashes:
            if not pipeline.has_cache(stage_hash):
                stale_hashes.add(stage_hash)

    # 4.6) Devalidate if parent has been updated
    for stage_hash in sorted_cached_hashes:
        if stage_hash not in stale_hashes and metadata.has(stage_hash):
            for (
                dependency_hash,
                dependency_update,
            ) in metadata.get_dependencies(stage_hash).items():
                if not metadata.has(dependency_hash):
                    stale_hashes.add(stage_hash)
                else:
                    if (
                        metadata.get_stage(dependency_hash)["updated"]
                        > dependency_update
                    ):
                        stale_hashes.add(stage_hash)

    # 4.7) Devalidate if parents are not the same anymore
    for stage_hash in sorted_cached_hashes:
        if stage_hash not in stale_hashes and metadata.has(stage_hash):
            cached_hashes = set(metadata.get_dependencies(stage_hash).keys())
            current_hashes = set(
                pipeline.get_dependencies(stage_hash)
                if "dependencies" in pipeline.get_stage(stage_hash)
                else []
            )

            if not cached_hashes == current_hashes:
                stale_hashes.add(stage_hash)

    # 4.8) Manually devalidate stages
    if working_directory:
        for stage_hash in sorted_cached_hashes:
            stage = pipeline.get_stage(stage_hash)
            cache_path = pipeline.get_stage_cache_dir(stage_hash)
            context = ValidateContext(stage["config"], cache_path)

            if not stage["wrapper"].validate(
                context
            ) == metadata.get_validation_token(stage_hash):
                stale_hashes.add(stage_hash)

    # 4.9) Devalidate descendants of devalidated stages
    for stage_hash in set(stale_hashes):
        for descendant_hash in nx.descendants(pipeline.graph, stage_hash):
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
    for stage_hash in stale_hashes:
        logger.info("- %s" % stage_hash)

    # 5) Reset meta information
    metadata.reset(stale_hashes)

    if working_directory is not None:
        metadata.update_json(working_directory)

    logger.info("Successfully reset meta data")

    # 6) Execute stages
    results = dict()
    progress = 0

    # General dependency cache to avoid loading the same cache in several
    # stages in a row.
    for stage_hash in sorted_hashes:
        if stage_hash in stale_hashes:
            result = pipeline.execute_stage(stage_hash)

            if stage_hash in required_hashes:
                results[stage_hash] = result

            progress += 1
            logger.info(
                f"Pipeline progress: {progress}/{len(stale_hashes)}"
                + f"({(100 * progress / len(stale_hashes)):.2f}%)"
            )

    if not rerun_required:
        # Load remaining previously cached results
        for stage_hash in required_hashes:
            if stage_hash not in results:
                results[stage_hash] = pipeline.load_cache(stage_hash)

    if verbose:
        info = {}

        for stage_hash in sorted(metadata.get_hashes()):
            info.update(metadata.get_info(stage_hash))

        return {
            "results": list(results.values()),
            "stale": stale_hashes,
            "info": info,
            "flowchart": pipeline.get_node_link_data(),
        }
    else:
        return list(results.values())
