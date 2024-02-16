#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License: BSD-3 (https://tldrlegal.com/license/bsd-3-clause-license-(revised))
# Copyright (c) 2024, JB Cabral, GVT-CONAE
# All rights reserved.

# =============================================================================
# DOCS
# =============================================================================

"""Multiple scripts for the GVT."""

# =============================================================================
# IMPORTS
# =============================================================================

import importlib_metadata


# =============================================================================
# CONSTANTS
# =============================================================================

NAME = "gvt_scripts"

DOC = __doc__

VERSION = importlib_metadata.version(NAME)

__version__ = tuple(VERSION.split("."))


del importlib_metadata
