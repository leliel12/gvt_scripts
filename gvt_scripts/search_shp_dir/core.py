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

import dbfread

import pyproj

from . import models
from ..utils import sr


# =============================================================================
# INTERNAL
# =============================================================================


def _store_dbf(
    *,
    db,
    mdir_path,
    dbf_path,
):
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
        reg = db.store_dbfreg(mdir_path, **record)
        records.append(reg)

    return records  # return the records


def _store_prj(*, db, mdir_path, prj_path):
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

    crs_data = crs.to_json_dict()
    crs_data["schema"] = crs_data.pop("$schema")

    db.store_prj(mdir_path, **crs_data)


def _store_jgw(*, db, mdir_path, jgw_path):
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
    # every jgw must has a jpg
    jpg_path = jgw_path.with_suffix(".jpg")

    if not jpg_path.exists():
        msg = f"Missing 'jpg' path for file {sr(jgw_path)}"
        raise ValueError(msg)

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
    jgw_data = {
        "path": str(jgw_path),
        "scale_x": jgw_lines_float[0],
        "rotation_y": jgw_lines_float[1],
        "rotation_x": jgw_lines_float[2],
        "scale_y": jgw_lines_float[3],
        "upper_left_x": jgw_lines_float[4],
        "upper_left_y": jgw_lines_float[5],
        "jpg_path": str(jpg_path),
    }

    db.store_jgw(mdir_path_or_reg=mdir_path, **jgw_data)


# =============================================================================
# PUBLIC FUNCTIONS
# =============================================================================


def store_mdir(path, db):

    mdir = db.get_or_create_mdir(path)

    dbf_path = mdir.path / f"{mdir.date_str}.dbf"
    _store_dbf(db=db, mdir_path=mdir.path, dbf_path=dbf_path)

    prj_path = path / f"{mdir.date_str}.prj"
    _store_prj(db=db, mdir_path=mdir.path, prj_path=prj_path)

    jgws = {}
    for jgw_path in path.glob("*.jgw"):
        _store_jgw(db=db, mdir_path=mdir.path, jgw_path=jgw_path)


def open_db(db_url=None):
    db = models.Database.from_url(db_url)
    return db


def populate_db(db, path):

    # list all dir and files with the name metadata
    metadata_dirs = path.glob("**/*metadata*/")
    for mdir in metadata_dirs:
        if not mdir.is_dir():  # ignore the non directory files
            continue
        store_mdir(mdir, db)

    return db
