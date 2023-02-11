"""Main module to run synpp from command line."""
import logging
import os
import sys

from synpp import PipelineError
from synpp import Synpp

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yml"

    if not os.path.isfile(config_path):
        raise PipelineError("Config file does not exist: %s" % config_path)

    Synpp.build_from_yml(config_path).run_pipeline()
