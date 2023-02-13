"""Pipeline metadata."""
import json
import os
import shutil


class PipelineMetadata:
    """Pipeline metadata."""

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
