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
Defines a core primitive structure of atri search: collections.
"""

import pickle
import shutil
import bisect

from typing import List, Callable, Set, Dict, Tuple
from pathlib import Path

import json

from atri.core.primitives.document import ADoc
from atri.core.constants import META_COL
from atri.core.parsing import get_parser
from atri.error import AtriError


class ACol:
    """
    A collection managed by atri is basically an abstraction that encapsulates a set of documents and an inverted index
    that allows future search processing.
    """

    def __init__(self, name: str, description: str, path: Path, **kwargs):
        """
        @param name: Corpus' name.
        @param description: Corpus' description.
        @param path: Directory where the collection is stored.
        Optional arguments
        replace: Allows replacement of documents with the same name.
        loading: Indicates if the collection is being loaded.
        atri_docs: In-memory  atri documents (It only works when loading is False).
        docs: List of documents stored in disk (It only works when loading is True).
        """

        # 1° Atribuição de atributos
        self._name: str = name
        self._description: str = description
        self._path: Path = path
        self._docs: Set[str] = set()  # conjunto dos documentos
        self._blocks: List[str] = list()  # paginação dos documentos
        self._index_defaults: Dict[str, str] = kwargs.get("index_defaults", {})
        self._search_defaults: Dict[str, str] = kwargs.get("search_defaults", {})
        self._lazy = True

        if kwargs.get('loading', False):
            if 'atri_docs' in kwargs:
                raise AtriError("Loading=True doesn't support ADoc lists.")

            if 'docs' in kwargs and kwargs['docs']:
                self._docs = kwargs['docs']  # KeyError if docs isn't present.
                self._lazy = False
        else:
            self._lazy = False

            if 'docs' in kwargs:
                raise AtriError("Loading=False doesn't support external document lists.")

            # 2°Criar corpus
            self._mkcol()

            # 3° Configurar corpus
            self._config_corpus(kwargs.get('atri_docs', None), replace=kwargs.get('replace', True))

    def __del__(self):
        try:
            self._update_metadata(replace=True)
        except:
            # ignore
            return

    def __str__(self) -> str:
        return f"<ACol {self._to_dict()}>"

    def __getitem__(self, doc: str):
        doc_path = self._path.joinpath(doc)
        if doc_path.exists():
            with doc_path.open(mode="rb") as io:
                atri_doc: ADoc = pickle.load(io)
                return atri_doc
        raise KeyError(f"Document {doc} not found.")

    def __contains__(self, doc):
        if self._lazy:
            return self._path.joinpath(doc).exists()
        return doc in self._docs

    def __iter__(self):
        if self._lazy:
            raise AtriError("The documents are lazily evaluated in this collection.")
        for doc in self._docs:
            doc_path = self._path.joinpath(doc)
            if doc_path.exists():
                with doc_path.open(mode="rb") as io:
                    atri_doc: ADoc = pickle.load(io)
                    yield atri_doc

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name
        self._update_metadata(replace=True)

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description
        self._update_metadata(replace=True)

    @property
    def index_defaults(self):
        return self._index_defaults

    @index_defaults.setter
    def index_defaults(self, defaults):
        self._index_defaults = defaults
        self._update_metadata(replace=True)

    @property
    def search_defaults(self):
        return self._search_defaults

    @search_defaults.setter
    def search_defaults(self, defaults):
        self._search_defaults = defaults
        self._update_metadata(replace=True)

    @property
    def path(self) -> Path:
        return self._path

    @path.setter
    def path(self, value):
        self._path = self._path.rename(value)

    @property
    def docs(self) -> Set[str]:
        return self._docs

    @docs.setter
    def docs(self, docs: Set[str]):
        for doc in docs:
            doc_path = self._path.joinpath(doc)
            if doc_path.exists():
                self._docs.add(doc)
                bisect.insort(self._blocks, doc)

        self._update_metadata(replace=True)

    def size(self):
        return self._path.stat().st_size / float(1024 ** 2)  # MB

    def get_docpath(self, doc: str):
        doc_path = self._path.joinpath(doc.strip())
        if doc_path.exists():
            return str(doc_path.absolute())
        raise ValueError(f"Invalid path for document: {doc}")

    def document_paths(self, page=1) -> Tuple[List[Path], int, int]:

        if self._lazy:
            self.force_load_docs()

        pths = []
        chunk_size = 10
        col_len = len(self._blocks)
        total_pages = round(col_len / chunk_size)

        if page == 0 or col_len == 0:
            for doc in self._docs:
                doc_path = self._path.joinpath(doc)
                if doc_path.exists():
                    pths.append(doc_path)
            return pths, 1, 1

        start = (page - 1) * chunk_size
        for doc in self._blocks[start: start + chunk_size]:
            doc_path = self._path.joinpath(doc)
            if doc_path.exists():
                pths.append(doc_path)

        return pths, page, total_pages

    def delete_document(self, doc: str) -> bool:
        """
        Deleta um documento da coleção
        """
        if self._lazy:
            self.force_load_docs()

        if doc in self._docs:
            p = self._path.joinpath(doc)
            if p.is_file():
                p.unlink()
                self._docs.remove(doc)
                self._blocks.remove(doc)
                return True
        return False

    def force_load_docs(self):
        if self._lazy:
            metadata = self._path.joinpath(META_COL).absolute()

            for p in self._path.iterdir():
                if p.is_file() and p.absolute() != metadata:
                    self._docs.add(p.name)
                    bisect.insort(self._blocks, p.name)

        self._lazy = False

    @staticmethod
    def delete(col: "ACol"):
        col._rmcol()
        del col

    @staticmethod
    def load_docs(path: Path) -> Set[str]:
        """
        Carrega os documentos de uma coleção
        """
        docs = set()
        metadata = path.joinpath(META_COL).absolute()
        for p in path.iterdir():
            if p.is_file() and p.absolute() != metadata:
                docs.add(p.name)
        return docs

    @staticmethod
    def load(path: Path, lazy=True) -> "ACol":
        """
        Carrega uma coleção de acordo com um diretório válido
        """
        if path.exists() and path.is_dir():
            metadata = path.joinpath(META_COL)
            if not metadata.exists():
                raise AtriError("The ACol's metadata is not found.")

            with metadata.open(mode='r', encoding='utf-8') as f:
                data = json.load(f)

            name = data['name']
            description = data['description']
            docs = ACol.load_docs(path) if not lazy else None

            col = ACol(name, description, path, loading=True, docs=docs, index_defaults=data['index_defaults'],
                       search_defaults=data['search_defaults'])

            return col

        raise AtriError("Invalid collection directory. Can't load properly.")

    def add(self, doc: ADoc):
        """
        Adiciona um documento ADoc em memória para o corpus
        """
        self._save_doc(doc, False)

    @staticmethod
    def doc_from_file(path: Path):
        if not path.is_file():
            raise AtriError("Invalid file")

        p = get_parser(path)

        atri_docs = p(path)

        return atri_docs

    def add_document_from_file(self, path: Path, parser: Callable[[Path], ADoc] = None, replace: bool = True):
        """
        Adiciona um arquivo externo para o corpus (cria-se a representação de memória primeiro com auxílio de um
        parser).
        Caso seja passado a variável nomeada io,  o path será utilizado para salvar o atri_doc,
        enquanto o seu conteúdo será extraído da variável nomeada.
        """

        if not path.is_file():
            raise AtriError("Invalid file")

        p = parser if parser else get_parser(path)

        atri_docs = p(path)

        if isinstance(atri_docs, ADoc):
            self.add(atri_docs)
        else:
            for atri_doc in atri_docs:
                self.add(atri_doc)

    def commit(self):
        self._update_metadata(replace=True)

    def json(self):
        return self._to_dict()

    def _to_dict(self):
        return \
            {
                "name": self._name,
                "description": self._description,
                "path": str(self._path.absolute()),
                "index_defaults": self._index_defaults,
                "search_defaults": self._search_defaults
            }

    def _mkcol(self):
        if self._path.is_dir():
            raise AtriError(f"The corpus points to a folder that already exists: {self._path.absolute()}.")
        self._path.mkdir(parents=True, exist_ok=False)

    def _rmcol(self):
        shutil.rmtree(self._path)

    def _save_doc(self, doc: ADoc, replace: bool):
        doc_path = self._path.joinpath(doc.name)

        if doc_path.exists() and not replace:
            raise AtriError(f"Document with name `{doc.name}` already exists.")

        with doc_path.open(mode="wb") as io:
            pickle.dump(doc, io)

        self._docs.add(doc.name)
        bisect.insort(self._blocks, doc.name)

    def _update_metadata(self, replace: bool):
        metadata = self._path.joinpath(META_COL)

        if (metadata.exists() and replace) or (not metadata.exists()):
            metadata.touch()
            with metadata.open(mode='w', encoding='utf-8') as f:
                json.dump(self._to_dict(), f, ensure_ascii=False)

    def _config_corpus(self, atri_docs: List[ADoc], replace: bool = True):

        # 1° dump todos atri_doc
        if atri_docs:
            for atri_doc in atri_docs:
                self._save_doc(atri_doc, replace)

        # 2° criar arquivo de metadados
        self._update_metadata(replace)
