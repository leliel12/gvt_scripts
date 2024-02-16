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
import tempfile
import zipfile


# Dependencies
import dbfread

import pyproj


# =============================================================================
# CONSTANTS
# =============================================================================

#: Project name is the name of the file without py!
PRJ_NAME = os.path.splitext(os.path.basename(__file__))[0]

#: List of file extensions to extract
EXTENSIONS = [".jgw"]


# =============================================================================
# ZIP HELPERS
# =============================================================================


def _zfile_ls(zfile, internal_dir_name, extensions):
    """Generate a dictionary of file entries with specified extensions within \
    a given internal directory of a zip file.

    Parameters
    ----------
    zfile : ZipFile
        The zip file object to be processed.
    internal_dir_name : str
        The name of the internal directory within the zip file.
    extensions : list of str
        A list of file extensions to filter for.

    Returns
    -------
    dict
        A dictionary where the keys are file extensions and the values are
        lists of file names with the corresponding extension.

    """
    entries = {ext: [] for ext in extensions}
    for filename in zfile.namelist():
        if os.path.dirname(filename) != internal_dir_name:
            continue
        ext = os.path.splitext(filename)[1]
        if ext in extensions or ext[1:] in extensions:
            entries[ext].append(filename)
    return entries


def _extract_to_tempdir(zfile, *, file_to_extract, temp_dir):
    """A function to extract a specified file from a zip file to a temporary \
    directory.

    Parameters
    ----------
    zfile : file-like
        The zip file object from which to extract the file.
    file_to_extract : str
        The name of the file to be extracted from the zip file.
    temp_dir : str
        The temporary directory to which the file will be extracted.

    Returns
    -------
    str
        The path to the temporary file where the specified file has been
        extracted.

    """
    # get the extension of the file to extract
    # so the temp file has the same extension
    ext = os.path.splitext(file_to_extract)[-1]

    # create a temp file with the same extension
    _, tempfile_path = tempfile.mkstemp(suffix=ext, dir=temp_dir)

    # extract the file to the temp file

    with open(tempfile_path, "wb") as dfp:
        dfp.write(zfile.read(file_to_extract))

    # return the temp file
    return str(tempfile_path)


# =============================================================================
# IN-ZIP FILE READER
# =============================================================================


def _read_dbf(zfile, dbf_path, temp_dir):
    """Reads a dbf file from a zip file, and returns the records as a list.

    Parameters
    ----------
    zfile : file-like object
        The zip file object containing the dbf file.
    dbf_path : str
        The path to the dbf file within the zip file.
    temp_dir : str
        The temporary directory to extract the dbf file.

    Returns
    -------
    dict
        A list containing the records from the dbf file.

    """
    temp_path = _extract_to_tempdir(
        zfile, file_to_extract=dbf_path, temp_dir=temp_dir
    )

    table = dbfread.DBF(temp_path)
    records = []
    for record in table:
        record.pop("filename", None)
        records.append(dict(record))

    return records  # return the records


def _read_prj(zfile, prj_path, temp_dir):
    """Reads a PRJ file from a zip file and returns its contents as a dict.

    Parameters
    ----------
    zfile : ZipFile
        The zip file object to read from.
    prj_path: str
        The path to the PRJ file within the zip file.
    temp_dir : str
        The temporary directory to save the PRJ file to.

    Returns
    -------
    dict
        The contents of the PRJ file as a dictionary

    """
    prj_bytes = zfile.read(prj_path)
    crs = pyproj.CRS(prj_bytes.decode())

    return crs.to_json_dict()


def _read_jgw(zf, jgw):
    """Reads a .jgw file from a zip file and extracts the 6 lines of data.

    Parameters
    ----------
    zf : ZipFile
        The zip file object.

    jgw : str
        The name of the .jgw file to be read from the zip file.

    Returns
    -------
    dict
        A dictionary containing the parsed jgw data.

    """
    jgw_bytes = zf.read(jgw)
    jgw_lines = jgw_bytes.decode().splitlines()

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
# API
# =============================================================================


def metadata_zshapefile_as_dict(path):
    """A function to extract metadata from a zipped shapefile and return it \
    as a dictionary.

    Parameters
    ----------
    path : str
        The file path of the zipped shapefile.

    Returns
    -------
    dict
        A dictionary containing the extracted data, including dbf, prj,
        and jgw files.

    """
    # nombre del directorio y nombre de archivos a buscar
    internal_dir_name = os.path.splitext(os.path.basename(path))[0]
    date_time_name = internal_dir_name.split("_")[1]

    # Create a temporary directory
    tempdir_suffix = f"_{internal_dir_name}_{PRJ_NAME}"
    with tempfile.TemporaryDirectory(suffix=tempdir_suffix) as temp_dir:

        # extract all the needed data
        data = {}
        with zipfile.ZipFile(path) as zf:

            dbf_path = os.path.join(internal_dir_name, f"{date_time_name}.dbf")
            data["dbf"] = _read_dbf(zf, dbf_path, temp_dir)

            prj_path = os.path.join(internal_dir_name, f"{date_time_name}.prj")
            data["prj"] = _read_prj(zf, prj_path, temp_dir)

            # list all multiple files to read
            ls = _zfile_ls(zf, internal_dir_name, EXTENSIONS)

            jgws = {}
            for jgw in ls[".jgw"]:
                jgws[jgw] = _read_jgw(zf, jgw)

            data["jgw"] = jgws

        return data


# =============================================================================
# CLI
# =============================================================================


def _main():
    pass


if __name__ == "__main__":
    _main()
