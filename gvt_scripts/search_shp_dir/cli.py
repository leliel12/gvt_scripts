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

import rich

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

        typer.secho(
            f"Created database for {sr(path)} -> {sr(db_url)}",
            fg=typer.colors.GREEN,
        )
        raise typer.Exit(code=0)

    def ssearch(
        self,
        db: pathlib.Path = typer.Option(..., help="URL to the dtabase file"),
        query: str = typer.Option(..., help="Query to search for"),
        to: typer.FileBinaryWrite = typer.Option(
            None, help="Path to the output file"
        ),
    ):
        """Simple search over a database.

        The query string should consist of one or more conditions in the format:
        "field operator value" separated by "&". Supported operators are: "=", "!=",
        "<", "<=", ">", ">=", "in", "not in".

        One example of a query is "satellite = 'Landsat-8' & cloudperce <= 10"

        The format are given by the "to" extension: Available formats {formats}

        """
        format = ".json" if to is None else pathlib.Path(to.name).suffix

        try:
            db_url = db
            db = core.open_db(db_url=db_url)
            records = db.simple_search(query)
            result = core.models.records_as_list(records)

            if to is None:
                rich.print(result)
            else:
                serializers.serialize(to, format, result)

        except Exception as err:
            typer.secho(str(err), fg=typer.colors.RED)
            raise typer.Exit(code=1)

    ssearch.__doc__ = ssearch.__doc__.format(
        formats=set(serializers.SERIALIZERS)
    )

    def fields(self):
        """List all fields available in a database."""
        db = core.open_db(db_url=None)
        fields = db.searcheable_fields_by_models()

        str_fields = {}
        for model, fields in fields.items():
            str_fields[model.__name__] = [fld.name for fld in fields]

        rich.print(str_fields)

    def fields_info(
        self,
        db: pathlib.Path = typer.Option(..., help="URL to the dtabase file"),
        fields: list[str] = typer.Argument(..., help="Fields names"),
    ):
        """Return an information about the type and posible values of a given fields."""



        db_url = db
        db = core.open_db(db_url=db_url)
        infos = [db.field_info(field) for field in fields]
        for info in infos:
            stats = info.pop("stats")
            field_name = info.pop("field_name")
            rich.print(rich.markdown.Markdown(f"# **Field:** {field_name!r}"))
            for k, v in info.items():
                k = k.replace("_", " ").title()
                rich.print(rich.markdown.Markdown(f"**{k}:** {v}"))

            rich.print(stats)





def main():
    """Run the CLI interface."""
    SearchSHPDir().run()
