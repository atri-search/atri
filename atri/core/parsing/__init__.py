# Copyright 2020 Marcos Pontes. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MARCOS PONTES ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MARCOS PONTES OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of MARCOS PONTES.

"""
Manages parsing functions implemented in this module.
"""
import importlib
import json
from typing import Callable, Union, Generator
from pathlib import Path

from atri.error import AtriError
from atri.core.primitives import ADoc

from atri.core.constants import PARSING, MULTIQUERY


class ParseError(AtriError):
    """
    Raised when a parsing function fails.
    """
    pass


def get_parser(path: Path, multiquery=False) -> Callable[[Path], Union[ADoc, Generator[ADoc, None, None]]]:
    """
    Returns a parser function for the given file path.
    :param path: Path to the file to be parsed.
    :param multiquery: If true, the parser will use multi-query mode.
    :return:
    """
    parsers = PARSING if not multiquery else MULTIQUERY
    extension = path.suffix

    if extension in parsers:

        if extension == '.trec':
            return __get_trec_parser(path, parsers[extension])

        else:
            module_name, class_name = parsers[extension].rsplit(".", 1)
            clazz = getattr(importlib.import_module(module_name), class_name)
            return clazz(path)

    raise ParseError(f"No such parser found for extension: {extension}")


def __get_trec_parser(trecfile, info):
    """
    Get the correct parser for a TREC file.
    :param trecfile: The path to the TREC file.
    :param info: dict with the information about the parser.
    :return:
    """
    with trecfile.open(mode='r', encoding='utf-8') as f:
        data = json.load(f)

    name = data.pop('name', None)
    corpus = data.pop('path', None)

    if 'parser' in data:
        module_name, class_name = data['parser'].rsplit(".", 1)
        clazz = getattr(importlib.import_module(module_name), class_name)
        return clazz(name, Path(corpus), **data)
    else:
        if name in info.keys():
            module_name, class_name = info[name].rsplit(".", 1)
            clazz = getattr(importlib.import_module(module_name), class_name)
            return clazz(name, Path(corpus), **data)
        else:
            raise ParseError(f"No such parser found for this collection")


__all__ = ['get_parser']
