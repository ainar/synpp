"""Pipeline utils."""
import inspect
import functools
import logging
from typing import Dict, List, Union, Callable
from types import ModuleType

import yaml

from .exceptions import PipelineError
from .stage import resolve_stage
from .run import run


class Synpp:
    """
    Convenience class mostly for running stages individually.

    (Possibly interactively, e.g. in Jupyter)
    """

    def __init__(
        self,
        config: dict,
        working_directory: str = None,
        logger: logging.Logger = logging.getLogger("synpp"),
        definitions: List[Dict[str, Union[str, Callable, ModuleType]]] = None,
        flowchart_path: str = None,
        dryrun: bool = False,
        externals: Dict[str, str] = {},
        aliases={},
    ):
        """Construct."""
        self.config = config
        self.working_directory = working_directory
        self.logger = logger
        self.definitions = definitions
        self.flowchart_path = flowchart_path
        self.dryrun = dryrun
        self.externals = externals
        self.aliases = aliases

    def run_pipeline(
        self,
        definitions=None,
        rerun_required=True,
        dryrun=None,
        verbose=False,
        flowchart_path=None,
    ):
        """Run the pipeline."""
        if definitions is None and self.definitions is None:
            raise PipelineError(
                "A list of stage definitions must be available in object or "
                + "provided explicitly."
            )
        elif definitions is None:
            definitions = self.definitions
        if dryrun is None:
            dryrun = self.dryrun
        return run(
            definitions,
            self.config,
            self.working_directory,
            flowchart_path=flowchart_path,
            dryrun=dryrun,
            verbose=verbose,
            logger=self.logger,
            rerun_required=rerun_required,
            ensure_working_directory=True,
            externals=self.externals,
            aliases=self.aliases,
        )

    def run_single(
        self,
        descriptor,
        config={},
        rerun_if_cached=False,
        dryrun=False,
        verbose=False,
    ):
        """Run a single stage."""
        return run(
            [{"descriptor": descriptor, "config": config}],
            self.config,
            self.working_directory,
            dryrun=dryrun,
            verbose=verbose,
            logger=self.logger,
            rerun_required=rerun_if_cached,
            flowchart_path=self.flowchart_path,
            ensure_working_directory=True,
            externals=self.externals,
            aliases=self.aliases,
        )[0]

    @staticmethod
    def build_from_yml(config_path):
        """Build pipeline from Yaml configuration file."""
        with open(config_path) as f:
            settings = yaml.load(f, Loader=yaml.SafeLoader)

        definitions = []

        for item in settings["run"]:
            parameters = {}

            if type(item) == dict:
                key = list(item.keys())[0]
                parameters = item[key]
                item = key

            definitions.append({"descriptor": item, "config": parameters})

        config = settings["config"] if "config" in settings else {}
        working_directory = (
            settings["working_directory"]
            if "working_directory" in settings
            else None
        )
        flowchart_path = (
            settings["flowchart_path"]
            if "flowchart_path" in settings
            else None
        )
        dryrun = settings["dryrun"] if "dryrun" in settings else False
        externals = settings["externals"] if "externals" in settings else {}
        aliases = settings["aliases"] if "aliases" in settings else {}

        return Synpp(
            config=config,
            working_directory=working_directory,
            definitions=definitions,
            flowchart_path=flowchart_path,
            dryrun=dryrun,
            externals=externals,
            aliases=aliases,
        )


def stage(function=None, *args, **kwargs):
    """Represent a stage with a function."""

    def decorator(_func):
        functools.wraps(_func)
        _func.stage_params = kwargs
        return _func

    # parameterized decorator
    if function is None:
        return decorator
    # parameterized decorator where a non-function stage is passed
    # this should be used like @stage(arg=stage("path.stage"))
    elif not inspect.isfunction(function):
        stage = resolve_stage(function)
        if stage is not None:
            stage.instance.stage_params = kwargs
            return stage.instance
        else:
            raise PipelineError(
                f"{function} could not be resolved as a stage."
            )
    else:  # unparameterized decorator
        return decorator(function)
