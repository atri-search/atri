
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

import os
import yaml

from functools import lru_cache

from atri.error import AtriError
from atri.config import get_or_else


@lru_cache
def load_configuration(config_path: str) -> dict:
    """
    Loads configuration from a YAML file.

    :param config_path: Path to the metadata file.
    :return: A dictionary with the metadata.
    """
    if not os.path.isfile(config_path):
        raise AtriError(f"Atri configuration file '{config_path}' not found.")

    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except KeyError:
        raise AtriError(f"Metadata file '{config_path}' is invalid.")


# ----------------------------------------------------------------------------- #
#                          Defining Constants                                   #
# ----------------------------------------------------------------------------- #

META_COL = get_or_else('META_COL', 'meta.json')  # < Name of the metadata file for each atri collection.
ATRI_CONFIG = get_or_else('ATRI_CONFIG', 'atri.yml')  # < Name of the atri configuration file.
N_PROC = get_or_else('N_PROC', 8)  # < Maximum number of processes to use.

# Loading metadata to extract more constants.
__config = load_configuration(ATRI_CONFIG)

DEFAULTS = __config.get('defaults', {})  # < Default values for each atri collection.
PARSING = __config.get('parsing', {})  # < Parsing rules for each atri collection.
MULTIQUERY = __config.get('multiquery', {})  # < Multi-query rules for each atri collection.
INDEXING = __config.get('index', {})  # < Indexing rules for each atri collection.
SIMILARITIES = __config.get('similarities', {})  # < Similarities rules for each atri collection.
METRICS = __config.get('metrics', {})  # < Metrics rules for each atri collection.


# ----------------------------------------------------------------------------- #
#                          MultiQuery Constants                                 #
# ----------------------------------------------------------------------------- #
DEFAULT_METRIC_LIST = ["ndcg@3", "ndcg@5", "ndcg@10", "ndcg@20", "precision@3", "precision@5", "precision@10",
                       "recall@3", "recall@5", "recall@10"]



