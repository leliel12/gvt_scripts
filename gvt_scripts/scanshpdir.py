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

import os
import pathlib
import pprint
import tempfile
import zipfile

# Dependencies
import attr

import dbfread

import joblib

import pyproj

import typer

# internal
from . import _base, serializers

# =============================================================================
# INTERNAL
# =============================================================================

def sr(obj):
    """Return the repr of the string representation of the given object.

    """
    return repr(str(obj))


def _read_dbf(dbf_path):
    """Reads a dbf file and returns the records as a list.

    Parameters
    ----------
    dbf_path : str
        The path to the dbf file.

    Returns
    -------
    list
        A list containing the records from the dbf file.

    """
    table = dbfread.DBF(dbf_path)
    records = []
    for record in table:
        record.pop("filename", None)
        records.append(dict(record))

    return records  # return the records


def _read_prj(prj_path):
    """Reads a PRJ file and returns its contents as a dict.

    Parameters
    ----------
    prj_path: str
        The path to the PRJ file.

    Returns
    -------
    dict
        The contents of the PRJ file as a dictionary

    """
    with open(prj_path) as fp:
        crs = pyproj.CRS(fp.read())

    return crs.to_json_dict()


def _read_jgw(jgw_path):
    """Reads a .jgw file and extracts the 6 lines of data.

    Parameters
    ----------
    jgw_path : str
        The path of the .jgw file to be read.

    Returns
    -------
    dict
        A dictionary containing the parsed jgw data.

    """
    with open(jgw_path) as fp:
        jgw_lines = fp.readlines()

    if len(jgw_lines) != 6:
        raise ValueError(
            "JGW file is not valid. "
            f"Must have 6 lines, instead has {len(jgw_lines)}"
        )

    jgw_lines_float = [
        float(line.strip().replace(",", ".", 1)) for line in jgw_lines
    ]

    # create a dictionary
    jgw_dict = {
        "scale_x": jgw_lines_float[0],
        "rotation_y": jgw_lines_float[1],
        "rotation_x": jgw_lines_float[2],
        "scale_y": jgw_lines_float[3],
        "upper_left_x": jgw_lines_float[4],
        "upper_left_y": jgw_lines_float[5],
    }

    return jgw_dict


# =============================================================================
# PUBLIC FUNCTIONS
# =============================================================================


def read_mdir_metadata(path):
    datetime_name = path.parent.name

    data = {}

    dbf_path = path / f"{datetime_name}.dbf"
    data["dbf"] = _read_dbf(dbf_path)

    prj_path = path / f"{datetime_name}.prj"
    data["prj"] = _read_prj(prj_path)

    jgws = {}
    for jgw_path in path.glob("*.jgw"):

        # every jgw must has a jpg
        jpg_path = jgw_path.with_suffix(".jpg")

        if not jpg_path.exists():
            msg = f"Missign 'jpg' path for file {sr(jgw_path)}"
            raise ValueError(msg)

        jgw_data = _read_jgw(jgw_path)
        jgw_data["jpg"] = jpg_path

        jgws[jgw_path] = jgw_data

    data["jgw"] = jgws

    return data


def metadata_as_dict(path):
    all_metadata = {}

    # list all dir and files with the name metadata
    metadata_dirs = path.glob("**/*metadata*/")
    for mdir in metadata_dirs:
        if not mdir.is_dir():  # ignore the non directory files
            continue
        all_metadata[str(mdir)] = read_mdir_metadata(mdir)

    return all_metadata


# =============================================================================
# CLI
# =============================================================================


@attr.s(frozen=True)
class ScanMetadataSHPDir(_base.CLIBase):
    """Zipped SHP metadata extractor."""

    def mkindex(
        self,
        path: pathlib.Path = typer.Argument(
            ..., help="Path to the directory."
        ),
        to: typer.FileBinaryWrite = typer.Option(
            ..., help="Path to the output index file"
        ),
    ):

        try:
            metadata = metadata_as_dict(path)
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
