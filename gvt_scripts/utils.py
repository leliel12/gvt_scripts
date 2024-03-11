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
