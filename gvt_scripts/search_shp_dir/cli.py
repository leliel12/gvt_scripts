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
class SearchSHPDir(cli_base.CLIBase):
    """SHP metadata extractor."""

    def mkdb(
        self,
        path: pathlib.Path = typer.Argument(
            ..., help="Path to the directory."
        ),
        db: pathlib.Path = typer.Option(
            help="URL to the dtabase file"
        ),
    ):
        """Create a database with given input and output paths."""
        db_url = db
        try:
            db = core.open_db(db_url=db_url)
            db = core.populate_db(path=path, db=db)
        except Exception as err:
            typer.echo(typer.style(str(err), fg=typer.colors.RED))
            raise typer.Exit(code=1)

        final_status = f"Created database for {sr(path)} -> {sr(db_url)}"
        typer.echo(typer.style(final_status, fg=typer.colors.GREEN))
        raise typer.Exit(code=0)

    def ssearch(
        self,
        db: pathlib.Path = typer.Option(..., help="URL to the dtabase file"),
        query: str = typer.Option(..., help="Query to search for"),
    ):
        """Simple search over a database."""
        # try:
        db_url = db
        db = core.open_db(db_url=db_url)
        result = db.simple_search(query)

    # except Exception as err:
    #     typer.echo(typer.style(str(err), fg=typer.colors.RED))
    #     raise typer.Exit(code=1)


def main():
    """Run the CLI interface."""
    SearchSHPDir().run()
