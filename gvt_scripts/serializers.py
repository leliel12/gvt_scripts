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

import dicttoxml

import orjson

import tomli_w

import yaml


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


# =============================================================================
# SERIALIZERS
# =============================================================================


@register(".json")
def to_json(d, stream):
    """Serialize a dictionary to JSON."""
    src = orjson.dumps(
        d, stream, option=orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2
    )
    stream.write(src)


@register(".yaml")
def to_yaml(d, stream):
    """Serialize a dictionary to YAML."""
    src = yaml.safe_dump(d).encode("utf-8")
    stream.write(src)


@register(".toml")
def to_toml(d, stream):
    """Serialize a dictionary to TOML."""
    tomli_w.dump(d, stream)


@register(".xml")
def to_xml(d, stream):
    """Serialize a dictionary to XML."""
    xml = dicttoxml.dicttoxml(d, custom_root="shap-zip")
    stream.write(xml)
