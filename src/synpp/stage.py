"""Stage classes."""
import inspect
import importlib
import sys
from typing import Callable

from .exceptions import PipelineError
from .progress import ProgressContext
from .parallel import ParallelMockMasterContext, ParallelMasterContext
from .functions import (
    hash_name,
    flatten,
    unflatten,
    NoDefaultValue,
    has_config_value,
    get_stage_hash,
)


def get_config_value(name, config):
    """Get value from configuration key."""
    flat = flatten(config)
    if name in flat:
        return flat[name]

    splitted_req = name.split(".")
    values = []
    keys = []
    for key in flat:
        found = True
        splitted_key = key.split(".")
        for idx in range(len(splitted_req)):
            if splitted_req[idx] != splitted_key[idx]:
                found = False
                break
        if found:
            keys.append(".".join(splitted_key[len(splitted_req) :]))
            values.append(flat[key])
    return unflatten(dict(zip(keys, values)))


class Context:
    """Parent class of contexts."""

    pass


class ConfigurationContext(Context):
    """Configuration context."""

    def __init__(
        self,
        base_config,
        config_definitions=[],
        externals={},
        global_aliases={},
    ):
        """Construct."""
        self.base_config = base_config
        self.config_requested_stages = [
            resolve_stage(d, externals, global_aliases).instance
            for d in config_definitions
        ]

        self.required_config = {}

        self.required_stages = []
        self.aliases = {}

        self.ephemeral_mask = []

        self.externals = externals
        self.global_aliases = global_aliases

    def config(self, option, default=NoDefaultValue()):
        """Require and get a config value."""
        if has_config_value(option, self.base_config):
            self.required_config[option] = get_config_value(
                option, self.base_config
            )
        elif not isinstance(default, NoDefaultValue):
            if (
                option in self.required_config
                and not self.required_config[option] == default
            ):
                raise PipelineError(
                    "Got multiple default values for config option: %s"
                    % option
                )

            self.required_config[option] = default

        if option not in self.required_config:
            raise PipelineError("Config option is not available: %s" % option)

        return self.required_config[option]

    def stage(self, descriptor, config={}, alias=None, ephemeral=False):
        """Set another stage as upstream."""
        definition = {"descriptor": descriptor, "config": flatten(config)}

        if definition not in self.required_stages:
            self.required_stages.append(definition)
            self.ephemeral_mask.append(ephemeral)

            if alias is not None:
                self.aliases[alias] = definition

    def stage_is_config_requested(self, descriptor):
        """Check if stage is requested in config."""
        if self.config_requested_stages:
            return (
                resolve_stage(
                    descriptor, self.externals, self.global_aliases
                ).instance
                in self.config_requested_stages
            )
        else:
            return False


class ValidateContext(Context):
    """Validate context."""

    def __init__(self, required_config, cache_path):
        """Construct."""
        self.required_config = required_config
        self.cache_path = cache_path

    def config(self, option):
        """Get configuration value."""
        if option not in self.required_config:
            raise PipelineError("Config option %s is not requested" % option)

        return self.required_config[option]

    def path(self):
        """Get stage cache path."""
        return self.cache_path


class ExecuteContext(Context):
    """Execute context."""

    def __init__(
        self,
        required_config,
        required_stages,
        aliases,
        working_directory,
        dependencies,
        cache_path,
        pipeline_config,
        logger,
        cache,
        dependency_info,
        dependency_cache,
    ):
        """Construct."""
        self.required_config = required_config
        self.working_directory = working_directory
        self.dependencies = dependencies
        self.pipeline_config = pipeline_config
        self.cache_path = cache_path
        self.logger = logger
        self.stage_info = {}
        self.dependency_cache = dependency_cache
        self.cache = cache
        self.dependency_info = dependency_info
        self.aliases = aliases
        self.required_stages = required_stages

        self.progress_context = None

    def config(self, option):
        """Get a configuration value from key."""
        if not has_config_value(option, self.required_config):
            raise PipelineError("Config option %s is not requested" % option)

        return get_config_value(option, self.required_config)

    def stage(self, name, config={}):
        """Get a stage result."""
        dependency = self._get_dependency(
            {"descriptor": name, "config": config}
        )

        if self.working_directory is None:
            return self.cache[dependency]
        else:
            return self.dependency_cache[dependency]

    def path(self, name=None, config={}):
        """Get the stage cache path."""
        if self.working_directory is None:
            raise PipelineError(
                "Cache paths don't work if no working directory was specified"
            )

        if name is None and len(config) == 0:
            return self.cache_path

        dependency = self._get_dependency(
            {"descriptor": name, "config": config}
        )
        return "%s/%s.cache" % (self.working_directory, dependency)

    def set_info(self, name, value):
        """Set stage info."""
        self.stage_info[name] = value

    def get_info(self, stage, name, config={}):
        """Get stage info."""
        dependency = self._get_dependency(
            {"descriptor": stage, "config": config}
        )

        if name not in self.dependency_info[dependency]:
            raise PipelineError(
                "No info '%s' available for %s" % (name, dependency)
            )

        return self.dependency_info[dependency][name]

    def parallel(
        self, data={}, processes=None, serialize=False, maxtasksperchild=None
    ):
        """Launch a parallel context."""
        config = self.required_config

        if processes is None and "processes" in self.pipeline_config:
            processes = self.pipeline_config["processes"]

        if serialize:
            # Add mock context to run all parallel tasks in series and in
            # the same process. This can be useful for debugging and especially
            # for profiling the code.
            return ParallelMockMasterContext(
                data, config, self.progress_context
            )
        else:
            return ParallelMasterContext(
                data,
                config,
                processes,
                self.progress_context,
                maxtasksperchild,
            )

    def progress(
        self, iterable=None, label=None, total=None, minimum_interval=1.0
    ):
        """Launch a progress context."""
        if (
            minimum_interval is None
            and "progress_interval" in self.pipeline_config
        ):
            minimum_interval = self.pipeline_config["progress_interval"]

        self.progress_context = ProgressContext(
            iterable, total, label, self.logger, minimum_interval
        )
        return self.progress_context

    def _get_dependency(self, definition):
        if definition["descriptor"] in self.aliases:
            if len(definition["config"]) > 0:
                raise PipelineError(
                    "Cannot define parameters for aliased stage"
                )

            definition = self.aliases[definition["descriptor"]]

        if definition not in self.required_stages:
            raise PipelineError(
                "Stage '%s' with parameters %s is not requested"
                % (definition["descriptor"], definition["config"])
            )

        return self.dependencies[self.required_stages.index(definition)]


def synpp_import_module(name, package=None, externals={}):
    """Import a module."""
    absolute_name = importlib.util.resolve_name(name, package)
    try:
        return sys.modules[absolute_name]
    except KeyError:
        pass

    if absolute_name in externals:
        spec = importlib.util.spec_from_file_location(
            absolute_name, externals[absolute_name]
        )
    else:
        spec = importlib.util.find_spec(absolute_name)
        if spec is None:
            msg = f"No module named {absolute_name!r}"
            raise ModuleNotFoundError(msg, name=absolute_name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[absolute_name] = module
    spec.loader.exec_module(module)
    return module


def resolve_stage(descriptor, externals: dict = {}, aliases: dict = {}):
    """
    Search stage given a descriptor.

    Supported descriptors: module, class, @stage-decorated function or
    stage-looking object.
    """
    if descriptor in aliases:
        descriptor = aliases[descriptor]

    if isinstance(descriptor, str):
        # If a string, first try to get the actual object
        try:
            descriptor = synpp_import_module(descriptor, externals=externals)
        except ModuleNotFoundError:
            try:
                parts = descriptor.split(".")
                module = synpp_import_module(
                    ".".join(parts[:-1]), externals=externals
                )
                descriptor = getattr(module, parts[-1])
            except (ValueError, ModuleNotFoundError):
                return None  # definitely not a stage

    if inspect.ismodule(descriptor):
        stage_hash = get_stage_hash(descriptor)
        return StageInstance(descriptor, descriptor.__name__, stage_hash)

    if inspect.isclass(descriptor):
        stage_hash = get_stage_hash(descriptor)
        return StageInstance(
            descriptor(),
            "%s.%s" % (descriptor.__module__, descriptor.__name__),
            stage_hash,
        )

    if inspect.isfunction(descriptor):
        if not hasattr(descriptor, "stage_params"):
            raise PipelineError(
                "Functions need to be decorated with @synpp.stage in order to"
                + "be used in the pipeline."
            )
        function_stage = DecoratedStage(
            execute_func=descriptor, stage_params=descriptor.stage_params
        )
        stage_hash = get_stage_hash(descriptor)
        return StageInstance(
            function_stage,
            "%s.%s" % (descriptor.__module__, descriptor.__name__),
            stage_hash,
        )

    if hasattr(descriptor, "execute"):
        # Last option: arbitrary object which looks like a stage
        clazz = descriptor.__class__
        stage_hash = get_stage_hash(clazz)
        return StageInstance(
            descriptor,
            "%s.%s" % (clazz.__module__, clazz.__name__),
            stage_hash,
        )

    # couldn't resolve stage (this is something else)
    return None


def configure_name(name, config):
    """Configure a name."""
    values = ["%s=%s" % (name, value) for name, value in config.items()]
    return "%s(%s)" % (name, ",".join(values))


class StageInstance:
    """Represents a stage instanciated."""

    def __init__(self, instance, name, module_hash):
        """Construct the stage instance."""
        self.instance = instance
        self.name = name
        self.module_hash = module_hash
        if not hasattr(self.instance, "execute"):
            raise RuntimeError(
                "Stage %s does not have execute method" % self.name
            )

    def parameterize(self, parameters):
        """Parametrize a stage."""
        # return ParameterizedStage(self.instance, self.name)
        raise NotImplementedError()

    def configure(self, context):
        """Configure the stage."""
        if hasattr(self.instance, "configure"):
            return self.instance.configure(context)

    def validate(self, context):
        """Validate the stage."""
        if hasattr(self.instance, "validate"):
            return self.instance.validate(context)

        return None

    def execute(self, context):
        """Execute the stage."""
        return self.instance.execute(context)


class ConfiguredStage:
    """Configured stage."""

    def __init__(self, instance, config, configuration_context):
        """Construct the stage."""
        self.instance = instance
        self.config = config
        self.configuration_context = configuration_context

        self.configured_name = configure_name(
            instance.name, configuration_context.required_config
        )
        self.hashed_name = hash_name(
            instance.name, configuration_context.required_config
        )

    def configure(self, context):
        """Configure."""
        if hasattr(self.instance, "configure"):
            return self.instance.configure(context)

    def execute(self, context):
        """Execute."""
        return self.instance.execute(context)

    def validate(self, context):
        """Validate."""
        return self.instance.validate(context)


class DecoratedStage:
    """Function as a stage implementation."""

    def __init__(self, execute_func: Callable, stage_params: dict):
        """Construct the stage."""
        self.execute_func = execute_func
        self.stage_params = stage_params
        self.func_params = inspect.signature(self.execute_func).parameters

    def configure(self, context):
        """Configure the stage."""
        self._resolve_params(context)

    def execute(self, context):
        """Execute the stage."""
        return self.execute_func(**self._resolve_params(context))

    def _resolve_params(self, context):
        """
        Resolve the parameters of the stage.

        This function is used both when configuring a stage as well as
        collecting the kwargs for execution.
        Stages may be decorated in three different ways:
        1. simple decoration:
                @synpp.stage
                def foo(bar='baz'): ...
            Which will look for 'bar' in the global context config (or else use
            he method's default if available).
        2. parameterized decoration:
                @synpp.stage(bar='bar_pipeline')
                def foo(bar): ...
            Which will try to decide whether:
             a. 'bar_pipeline' is an available stage, and pass the method its
             results, or
             b. an argument in the global context config (or else use the
             method's default if available).
             Note: the default value for 'bar' must be set in the actual
             function, not in the decorator.
        3. fully parameterized decoration, which can be used for configuring
                the stage dependencies:
                @synpp.stage(bar=synpp.stage('pipeline.bar',
                bar_argkey='bar_argval'))
                def foo(bar): ...
            Using synpp.stage() inside the decorator works in the same way as
            the decorator itself, simply pass the target stage as first
            argument, and the config kwargs following it.
        """
        func_kwargs = {}
        for name, param in self.func_params.items():
            arg = self.stage_params.get(name)
            if arg is None:
                # case 1: simple decoration
                config_kwargs = {"option": name}
                if (
                    isinstance(context, ConfigurationContext)
                    and param.default is not inspect._empty
                ):
                    config_kwargs.update({"default": param.default})
                func_kwargs[name] = context.config(**config_kwargs)
            elif resolve_stage(arg) is not None:
                # case 2 or 3: stages only
                func_kwargs[name] = context.stage(
                    arg, config=getattr(arg, "stage_params", {})
                )
            else:
                # case 2: config parameter
                func_kwargs[name] = context.config(arg)

        return func_kwargs
