"""Class file for Pipeline class."""
import os
import json
import pickle
import networkx as nx
import multiprocessing as mp
import datetime
from networkx.readwrite.json_graph import node_link_data

from .exceptions import PipelineError
from .functions import rmtree
from .stage import ExecuteContext, ValidateContext
from .pipeline_metadata import PipelineMetadata


def execute_wrapper(func, context, queue: mp.Queue):
    result = func(context)
    queue.put((result, context.stage_info))


class Pipeline:
    """Represents all the pipeline related methods and attributes."""

    def __init__(self, logger) -> None:
        self.logger = logger
        self.node_link_data = None
        self.progress_interval = None
        self.processes = None
        self.working_directory = None
        self.cache_available = set()
        self.metadata: PipelineMetadata = None
        self.cache = dict()

    def set_metadata(self, metadata):
        self.metadata = metadata

    def set_progress_interval(self, progress_interval) -> None:
        self.progress_interval = progress_interval

    def set_working_directory(self, working_directory) -> None:
        self.working_directory = working_directory

    def set_processes(self, processes) -> None:
        self.processes = processes

    def set_registry(self, registry):
        self.registry = registry

    def get_stages(self):
        return self.registry.values()

    def get_stage(self, hash):
        return self.registry.get(hash)

    def get_stage_hashes(self):
        return self.registry.keys()

    def get_stages_count(self):
        return len(self.registry)

    def get_sorted_hashes(self):
        return list(nx.topological_sort(self.graph))

    def construct_graph(self):
        self.graph = nx.DiGraph()
        self.flowchart = nx.MultiDiGraph()  # graph to later plot

        for hash in self.get_stage_hashes():
            self.graph.add_node(hash)

        for stage in self.get_stages():
            stage_name = stage["descriptor"]

            if not self.flowchart.has_node(stage_name):
                self.flowchart.add_node(stage_name)

            for hash in stage["dependencies"]:
                self.graph.add_edge(hash, stage["hash"])

                dependency_name = self.get_stage(hash)["descriptor"]
                if not self.flowchart.has_edge(dependency_name, stage_name):
                    self.flowchart.add_edge(dependency_name, stage_name)

    def generate_node_link_data(self):
        self.node_link_data = node_link_data(self.flowchart)

    def get_node_link_data(self):
        if self.node_link_data is None:
            self.generate_node_link_data()
        return self.node_link_data

    def output_flowchart(self, flowchart_path):
        flowchart_directory = os.path.dirname(os.path.abspath(flowchart_path))
        if not os.path.isdir(flowchart_directory):
            raise PipelineError(
                f"Flowchart directory does not exist: {flowchart_directory}"
            )

        self.logger.info(
            f"Writing pipeline flowchart to {flowchart_directory}"
        )
        with open(flowchart_path, "w") as outfile:
            json.dump(self.get_node_link_data(), outfile)

    def set_ephemeral(self, hash, flag):
        self.registry[hash]["ephemeral"] = flag

    def is_ephemeral(self, hash):
        return self.registry[hash]["ephemeral"]

    def detect_cycles(self):
        for cycle in nx.cycles.simple_cycles(self.graph):
            # TODO: Make more verbose
            cycle = [self.get_stage(hash)["hash"] for hash in cycle]
            raise PipelineError("Found cycle: %s" % " -> ".join(cycle))

    def get_config(self):
        config = {}
        if self.processes is not None:
            config["processes"] = self.processes
        if self.progress_interval is not None:
            config["progress_interval"] = self.progress_interval
        return config

    def check_available_cache(self):
        if self.working_directory is None:
            return
        for hash in self.get_sorted_hashes():
            dir_path = self.get_stage_cache_dir(hash)
            file_path = self.get_stage_cache_file(hash)

            if os.path.exists(dir_path) and os.path.exists(file_path):
                self.cache_available.add(hash)
                self.set_ephemeral(hash, False)

    def prepare_ephemeral(self):
        self.ephemeral_counts = dict()
        for stage in self.get_stages():
            for hash in stage["dependencies"]:
                if (
                    self.is_ephemeral(hash)
                    and hash not in self.cache_available
                ):
                    if hash not in self.ephemeral_counts:
                        self.ephemeral_counts[hash] = 0

                    self.ephemeral_counts[hash] += 1

    def get_ephemeral_stages(self):
        return self.ephemeral_counts.keys()

    def get_module_hash(self, hash):
        return self.registry[hash]["wrapper"].module_hash

    def has_cache(self, hash):
        return hash in self.cache_available

    def get_stage_cache_dir(self, hash):
        return os.path.join(self.working_directory, hash + ".cache")

    def get_stage_cache_file(self, hash):
        return os.path.join(self.working_directory, hash + ".p")

    def load_cache(self, hash):
        with open(self.get_stage_cache_file(hash), "rb") as f:
            self.logger.info(f"Loading cache for {hash}...")
            return pickle.load(f)

    def clear_ephemerals(self, dependencies):
        for dependency_hash in dependencies:
            if dependency_hash in self.ephemeral_counts:
                self.ephemeral_counts[dependency_hash] -= 1

                if self.ephemeral_counts[dependency_hash] == 0:
                    rmtree(self.get_stage_cache_dir(dependency_hash))
                    os.remove(self.get_stage_cache_file(dependency_hash))

                    self.logger.info(f"Removed ephemeral {dependency_hash}.")
                    del self.ephemeral_counts[dependency_hash]

    def save_cache(self, hash, result):
        with open(self.get_stage_cache_file(hash), "wb+") as f:
            self.logger.info(f"Writing cache for {hash}")
            pickle.dump(result, f, protocol=5)

    def get_dependencies(self, hash):
        return self.registry[hash]["dependencies"]

    def execute_stage(self, stage_hash):
        self.logger.info(f"Executing stage {stage_hash}...")
        stage = self.get_stage(stage_hash)

        # Delete useless cache
        for dependency_definition in list(self.cache.keys()):
            if (
                dependency_definition not in stage["dependencies"]
                and self.working_directory is not None
            ):
                del self.cache[dependency_definition]

        # Load stage dependencies and dependency infos
        stage_dependency_info = {}
        for dependency_hash in stage["dependencies"]:
            stage_dependency_info[dependency_hash] = self.metadata.get_info(
                dependency_hash
            )
            if (
                self.working_directory is not None
                and dependency_hash not in self.cache
            ):
                self.cache[dependency_hash] = self.load_cache(
                    dependency_hash
                )

        # Prepare cache path
        if self.working_directory is None:
            cache_path = None
        else:
            cache_path = self.get_stage_cache_dir(stage_hash)
            if os.path.exists(cache_path):
                rmtree(cache_path)
            os.mkdir(cache_path)

        context = ExecuteContext(
            stage["config"],
            stage["required_stages"],
            stage["aliases"],
            self.working_directory,
            stage["dependencies"],
            cache_path,
            self.get_config(),
            self.logger,
            self.cache,
            stage_dependency_info,
        )
        result_queue = mp.Queue(maxsize=1)

        process = mp.Process(
            target=execute_wrapper,
            args=(stage["wrapper"].execute, context, result_queue),
        )
        process.start()
        result, stage_info = result_queue.get()

        validation_token = stage["wrapper"].validate(
            ValidateContext(stage["config"], cache_path)
        )

        self.cache[stage_hash] = result

        # Update meta information
        self.metadata.set_data(
            stage_hash,
            stage["config"],
            datetime.datetime.utcnow().timestamp(),
            stage["dependencies"],
            stage_info,
            validation_token,
            stage["wrapper"].module_hash,
        )

        if self.working_directory is not None:
            self.save_cache(stage_hash, result)
            self.metadata.update_json(self.working_directory)
            # Clear cache for ephemeral stages if they are no longer needed
            self.clear_ephemerals(stage["dependencies"])

        self.logger.info(f"Finished running {stage_hash}.")

        return result
