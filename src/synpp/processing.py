"""Process stages."""
import copy

from .functions import flatten, hash_name
from .exceptions import PipelineError
from .stage import ConfigurationContext, resolve_stage


def process_stages(definitions, global_config, externals={}, aliases={}):
    """Process stages."""
    pending = copy.copy(definitions)
    stages = []

    for index, stage in enumerate(pending):
        stage["required-index"] = index

    global_config = flatten(global_config)

    while len(pending) > 0:
        definition = pending.pop(0)

        # Resolve the underlying code of the stage
        wrapper = resolve_stage(definition["descriptor"], externals, aliases)
        if wrapper is None:
            raise PipelineError(
                f"{definition['descriptor']} is not a supported object for "
                + "pipeline stage definition!"
            )

        # Call the configure method of the stage and obtain parameters
        config = copy.copy(global_config)

        if "config" in definition:
            config.update(definition["config"])

        # Obtain configuration information through configuration context
        context = ConfigurationContext(
            config, [d["descriptor"] for d in definitions], externals
        )
        wrapper.configure(context)
        required_config = flatten(context.required_config)
        definition = copy.copy(definition)
        definition.update(
            {
                "wrapper": wrapper,
                "config": copy.copy(required_config),
                "required_config": copy.copy(required_config),
                "required_stages": context.required_stages,
                "aliases": context.aliases,
            }
        )

        # Check for cycles
        cycle_hash = hash_name(
            definition["wrapper"].name, definition["config"]
        )

        if (
            "cycle_hashes" in definition
            and cycle_hash in definition["cycle_hashes"]
        ):
            print(definition["cycle_hashes"])
            print(cycle_hash)
            raise PipelineError(
                "Found cycle in dependencies: %s" % definition["wrapper"].name
            )

        # Everything fine, add it
        stages.append(definition)
        stage_index = len(stages) - 1

        # Process dependencies
        for position, upstream in enumerate(context.required_stages):
            passed_parameters = set(flatten(upstream["config"]).keys())

            upstream_config = copy.copy(config)
            upstream_config.update(upstream["config"])

            cycle_hashes = (
                copy.copy(definition["cycle_hashes"])
                if "cycle_hashes" in definition
                else []
            )
            cycle_hashes.append(cycle_hash)

            upstream = copy.copy(upstream)
            upstream.update(
                {
                    "config": upstream_config,
                    "downstream-index": stage_index,
                    "downstream-position": position,
                    "downstream-length": len(context.required_stages),
                    "downstream-passed-parameters": passed_parameters,
                    "cycle_hashes": cycle_hashes,
                    "ephemeral": context.ephemeral_mask[position]
                    or ("ephemeral" in definition and definition["ephemeral"]),
                }
            )
            pending.append(upstream)

    # Now go backwards in the tree to find intermediate config requirements
    # and set up dependencies
    downstream_indices = set(
        [
            stage["downstream-index"]
            for stage in stages
            if "downstream-index" in stage
        ]
    )

    source_indices = set(range(len(stages))) - downstream_indices

    # Connect downstream stages with upstream stages via dependency field
    pending = list(source_indices)

    while len(pending) > 0:
        stage_index = pending.pop(0)
        stage = stages[stage_index]

        if "downstream-index" in stage:
            downstream = stages[stage["downstream-index"]]

            # Connect this stage with the downstream stage
            if "dependencies" not in downstream:
                downstream["dependencies"] = [None] * stage[
                    "downstream-length"
                ]

            downstream["dependencies"][
                stage["downstream-position"]
            ] = stage_index

            pending.append(stage["downstream-index"])

    # Update configuration requirements based dependencies
    pending = list(source_indices)

    while len(pending) > 0:
        stage_index = pending.pop(0)
        stage = stages[stage_index]

        if "dependencies" in stage:
            passed_config_options = {}

            for upstream_index in stage["dependencies"]:
                upstream = stages[upstream_index]
                explicit_config_keys = (
                    upstream["downstream-passed-parameters"]
                    if "downstream-passed-parameters" in upstream
                    else set()
                )

                for key in upstream["config"].keys() - explicit_config_keys:
                    value = upstream["config"][key]

                    if key in passed_config_options:
                        assert passed_config_options[key] == value
                    else:
                        passed_config_options[key] = value

            for key, value in passed_config_options.items():
                if key in stage["config"]:
                    assert stage["config"][key] == value
                else:
                    stage["config"][key] = value

        if "downstream-index" in stage:
            pending.append(stage["downstream-index"])

    # Hash all stages
    required_hashes = {}

    for stage in stages:
        stage["hash"] = hash_name(stage["wrapper"].name, stage["config"])

        if "required-index" in stage:
            index = stage["required-index"]

            if stage["hash"] in required_hashes:
                assert required_hashes[stage["hash"]] == index
            else:
                required_hashes[stage["hash"]] = index

    # Reset ephemeral stages
    ephemeral_hashes = set([stage["hash"] for stage in stages]) - set(
        [
            stage["hash"]
            for stage in stages
            if "ephemeral" not in stage or not stage["ephemeral"]
        ]
    )
    for stage in stages:
        stage["ephemeral"] = stage["hash"] in ephemeral_hashes

    # Collapse stages again by hash
    registry = {}

    for stage in stages:
        registry[stage["hash"]] = stage

        stage["dependencies"] = (
            [stages[index]["hash"] for index in stage["dependencies"]]
            if "dependencies" in stage
            else []
        )

    for hash in required_hashes:
        registry[hash]["required-index"] = required_hashes[hash]

    return registry
