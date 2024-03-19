#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License: BSD-3 (https://tldrlegal.com/license/bsd-3-clause-license-(revised))
# Copyright (c) 2024, JB Cabral, GVT-CONAE
# All rights reserved.

# =============================================================================
# DOCS
# =============================================================================

"""Scandirectory of satellite observation create a indexed metadata db and \
provide search capabilities."""

# =============================================================================
# IMPORTS
# =============================================================================

from .cli import main
from .core import open_db, populate_db, store_mdir

__all__ = ["main"]
