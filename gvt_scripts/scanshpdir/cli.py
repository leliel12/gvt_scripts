#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License: BSD-3 (https://tldrlegal.com/license/bsd-3-clause-license-(revised))
# Copyright (c) 2024, JB Cabral, GVT-CONAE
# All rights reserved.

# =============================================================================
# DOCS
# =============================================================================

"""Extract metadata from shapefiles."""

# =============================================================================
# IMPORTS
# =============================================================================

import pathlib

import attr

import joblib

import typer

# internal
from . import core
from .. import cli_base
from ..utils import sr


# =============================================================================
# CLI
# =============================================================================


@attr.s(frozen=True)
class ScanMetadataSHPDir(cli_base.CLIBase):
    """SHP metadata extractor."""

    def mkdb(
        self,
        path: pathlib.Path = typer.Argument(
            ..., help="Path to the directory."
        ),
        to: typer.FileBinaryWrite = typer.Option(
            ..., help="Path to the output index file"
        ),
    ):
        import ipdb

        ipdb.set_trace()
        try:
            metadata = core.metadata_as_dict(path)
            joblib.dump(metadata, to)
        except Exception as err:
            typer.echo(typer.style(str(err), fg=typer.colors.RED))
            raise typer.Exit(code=1)

        final_status = f"Created index for {sr(path)} -> {sr(to.name)}"
        typer.echo(typer.style(final_status, fg=typer.colors.GREEN))
        raise typer.Exit(code=0)


def main():
    """Run the CLI interface."""
    ScanMetadataSHPDir().run()
