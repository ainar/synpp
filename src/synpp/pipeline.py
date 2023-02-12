"""Class file for Pipeline class."""
import os
import json
import shutil
import pickle
import networkx as nx
from networkx.readwrite.json_graph import node_link_data

from .exceptions import PipelineError
from .functions import rmtree


class Pipeline:
    """Represents all the pipeline related methods and attributes."""

    def __init__(self, logger) -> None:
        self.logger = logger
        self.node_link_data = None
        self.progress_interval = None
        self.processes = None
        self.working_directory = None
        self.cache_available = set()

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


class PipelineMetadata:
    def __init__(self, logger) -> None:
        self.logger = logger
        self.meta = {}

    def try_load_metadata(self, working_directory):
        path = os.path.join(working_directory, "pipeline.json")
        try:
            with open(path) as f:
                self.meta = json.load(f)
                self.logger.info(f"Found pipeline metadata in {path}")
        except FileNotFoundError:
            self.logger.info(f"Did not find pipeline metadata in {path}")

    def get_hashes(self):
        return self.meta.keys()

    def get_module_hash(self, hash):
        return self.meta[hash]["module_hash"]

    def get_stage(self, hash):
        return self.meta[hash]

    def get_dependencies(self, hash):
        return self.meta[hash]["dependencies"]

    def has(self, hash):
        return hash in self.meta

    def update_json(self, working_directory):
        """Update pipeline file."""
        if os.path.exists("%s/pipeline.json" % working_directory):
            shutil.move(
                "%s/pipeline.json" % working_directory,
                "%s/pipeline.json.bk" % working_directory,
            )

        with open("%s/pipeline.json.new" % working_directory, "w+") as f:
            json.dump(self.meta, f)

        shutil.move(
            "%s/pipeline.json.new" % working_directory,
            "%s/pipeline.json" % working_directory,
        )

    def reset(self, hashes):
        for hash in hashes:
            if self.has(hash):
                del self.meta[hash]

    def get_validation_token(self, hash):
        try:
            return self.meta[hash]["validation_token"]
        except KeyError:
            return None

    def get_info(self, hash):
        return self.meta[hash]["info"]

    def set_data(
        self,
        hash,
        config,
        updated,
        dependencies,
        info,
        validation_token,
        module_hash,
    ):
        self.meta[hash] = {
            "config": config,
            "updated": updated,
            "dependencies": {
                dependency_hash: self.get_stage(dependency_hash)["updated"]
                for dependency_hash in dependencies
            },
            "info": info,
            "validation_token": validation_token,
            "module_hash": module_hash,
        }
