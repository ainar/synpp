"""Common functions for Synpp."""
import hashlib
import copy
import inspect
import json
from collections.abc import MutableMapping


def flatten(d, parent_key="", sep="."):
    """Flatten a nested dict."""
    items = []
    for k, v in d.items():
        new_key = str(parent_key) + sep + str(k) if parent_key else str(k)
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return copy.deepcopy(dict(items))


def unflatten(flatten, sep="."):
    """Unflatten a nested dict."""
    out = {}
    for key, value in flatten.items():
        parts = key.split(sep)
        d = out
        for part in parts[:-1]:
            if part.isdigit():
                part = int(part)
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value
    return copy.deepcopy(out)


class NoDefaultValue:
    """No default value."""

    pass


def get_stage_hash(descriptor):
    """Generate a stage hash given its source code."""
    source = inspect.getsource(descriptor)
    hash = hashlib.md5()
    hash.update(source.encode("utf-8"))
    return hash.hexdigest()


def has_config_value(name, config):
    """Check if configuration key is available."""
    splitted_req = name.split(".")
    for key in flatten(config):
        found = True
        spltted_key = key.split(".")
        for idx in range(len(splitted_req)):
            if splitted_req[idx] != spltted_key[idx]:
                found = False
                break
        if found:
            return True
    return False


def hash_name(name, config):
    """Get hash name of a stage given its name and its config."""
    if len(config) > 0:
        hash = hashlib.md5()
        hash.update(json.dumps(config, sort_keys=True).encode("utf-8"))
        return "%s__%s" % (name, hash.hexdigest())
    else:
        return name
