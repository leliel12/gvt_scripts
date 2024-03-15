#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License: BSD-3 (https://tldrlegal.com/license/bsd-3-clause-license-(revised))
# Copyright (c) 2024, JB Cabral, GVT-CONAE
# All rights reserved.

# =============================================================================
# DOCS
# =============================================================================

"""Multiple utilities."""

# =============================================================================
# IMPORTS
# =============================================================================

import atexit
import shutil
import tempfile
from collections import abc as cabc

# =============================================================================
# CONSTANTS
# =============================================================================

TEMP_DIR = tempfile.TemporaryDirectory(suffix="_gvt_scripts")
atexit.register(shutil.rmtree, TEMP_DIR.name)


# =============================================================================
# NOT IMPLEMENTHED
# =============================================================================


class _Undefined:
    def __new__(cls, *a, **kws):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls, *a, **kws)
        return cls._instance

    def __repr__(self):
        return f"<{type(self).__name__}"


Undefined = _Undefined()


# =============================================================================
# FUNCTIONS
# =============================================================================


def sr(obj):
    """Return the repr of the string representation of the given object."""
    return repr(str(obj))


def flatten(obj, key="", separator="."):
    """Recursively flatten a nested object by combining nested keys with a
    specified separator.

    Parameters
    ----------
    obj : dict
        The nested object to be flattened.
    key : str, optional
        The current key being processed (default is an empty string).
    separator : str, optional
        The separator to use when combining keys (default is a dot ".").

    Returns
    -------
    dict
        A dictionary containing the flattened key-value pairs of the input
        object.
    """
    items = []
    if isinstance(obj, cabc.Sequence) and not isinstance(obj, str):
        for idx_item, item in enumerate(obj):
            subkey = (
                separator.join([key, str(idx_item)]) if key else str(idx_item)
            )
            items.extend(flatten(item, subkey, separator=separator).items())
    elif isinstance(obj, cabc.MutableMapping):

        for k_item, v_item in obj.items():
            sub_key = (
                separator.join([key, str(k_item)]) if key else str(k_item)
            )
            items.extend(flatten(v_item, sub_key, separator=separator).items())
    else:
        items.append((key, obj))
    return dict(items)
