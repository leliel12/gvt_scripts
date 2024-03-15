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
import sys

import attr

import joblib

import typer

# internal
from . import core
from .. import cli_base
from ..utils import sr
from .. import serializers


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
        db: pathlib.Path = typer.Option(help="URL to the dtabase file"),
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
        to: typer.FileBinaryWrite = typer.Option(
            None, help="Path to the output file"
        ),
    ):
        """Simple search over a database."""
        format = ".json" if to is None else pathlib.Path(to.name).suffix
        to = sys.stdout if to is None else to

        try:
            db_url = db
            db = core.open_db(db_url=db_url)
            records = db.simple_search(query)
            result = core.models.records_as_list(records)
            serializers.serialize(to, format, result)

            if to is sys.stdout():
                sys.stdout.write("\n")
            sys.stdout.flush()

        except Exception as err:
            typer.echo(typer.style(str(err), fg=typer.colors.RED))
            raise typer.Exit(code=1)

    def fields(self):
        """List all fields available in a database."""
        db = core.open_db(db_url=None)
        fields = db.searcheable_fields_by_models()

        str_fields = {}
        for model, fields in fields.items():
            str_fields[model.__name__] = [fld.name for fld in fields]

        serializers.serialize(sys.stdout, ".json", str_fields)
        sys.stdout.write("\n")
        sys.stdout.flush()


def main():
    """Run the CLI interface."""
    SearchSHPDir().run()
