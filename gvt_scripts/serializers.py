#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License: BSD-3 (https://tldrlegal.com/license/bsd-3-clause-license-(revised))
# Copyright (c) 2024, JB Cabral, GVT-CONAE
# All rights reserved.

# =============================================================================
# DOCS
# =============================================================================

"""Serializers for GVT scripts.

Todos estas funciones reciben un dictionario y un stream y escriben el dict
en el stream con el formato deseado.

"""

# =============================================================================
# IMPORTS
# =============================================================================

import csv
import io

import dicttoxml

import orjson

import tomli_w

import yaml

from . import utils


# =============================================================================
# REGISTERS
# =============================================================================

SERIALIZERS = {}


def register(ext):
    """Register a serializer for a given extension."""

    def decorator(func):
        SERIALIZERS[ext] = func
        return func

    return decorator


def serialize(stream, format, obj):
    """Serialize a dict/list/scalar to a given format.

    Also ensures that the stream is in text mode.

    """
    if stream.mode == "wb":
        stream = io.TextIOWrapper(stream, encoding="utf-8")
    SERIALIZERS[format](obj, stream)
    stream.flush()


# =============================================================================
# SERIALIZERS
# =============================================================================


@register(".json")
def to_json(d, stream):
    """Serialize a dict/list/scalar to JSON."""
    src = orjson.dumps(
        d, stream, option=orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2
    )
    stream.write(src.decode("utf-8"))


@register(".yaml")
@register(".yml")
def to_yaml(d, stream):
    """Serialize a dict/list/scalar to YAML."""
    src = yaml.safe_dump(d, indent=2)
    stream.write(src)


@register(".xml")
def to_xml(d, stream):
    """Serialize a dict/list/scalar to XML."""
    xml = dicttoxml.dicttoxml(d, custom_root="data").decode("utf-8")
    stream.write(xml)


# CSV =========================================================================


@register(".csv")
def to_csv(d, stream):
    """Serialize a dict/list/scalar to csv."""

    d = list(map(utils.flatten, d)) if isinstance(d, list) else d[0]
    fieldnames = list(d[0])

    escritor_csv = csv.DictWriter(stream, fieldnames=fieldnames)
    escritor_csv.writeheader()
    escritor_csv.writerows(d)
