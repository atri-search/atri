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
Module that index AtriCols.
"""

import os
import shutil
from pathlib import Path

from whoosh import index
from whoosh.fields import Schema, ID, TEXT

from atri.core import ACol, ADoc
from atri.core.similarities.pagerank import PageRank, Adjacency, normalize_title


class AIndexer(object):
    def __init__(self, collection: ACol, **kwargs):
        self.collection = collection
        self.index_path = self._resolve_index_path()
        self.graph_path = self._resolve_graph_path()
        try:
            self._index = index.open_dir(self.index_path, schema=self.get_schema(**kwargs))
        except Exception:
            self._index = index.create_in(self.index_path, schema=self.get_schema(**kwargs))

    def _resolve_index_path(self):
        index_path = self.collection.path.joinpath("_index")
        index_path.mkdir(parents=True, exist_ok=True)
        return index_path

    def _resolve_graph_path(self):
        graph_path = self.collection.path.joinpath("_graph")
        graph_path.mkdir(parents=True, exist_ok=True)
        return graph_path

    def searcher(self, **kwargs):
        return self._index.searcher(**kwargs)

    def index(self, *, clean=False, graph=False, link_field='links'):
        # We must load the collection before indexing if the collection was lazy loaded
        self.collection.force_load_docs()
        if clean:
            self.clean_index(graph, link_field)
        else:
            self.incremental_index()

    @classmethod
    def get_schema(cls, **kwargs):
        """
        Obtém o esquema do índice do atri
        Investigar se vector e stored estão deixando muito lento.
        """
        from atri.core.constants import INDEXING
        index_data = INDEXING

        return Schema(name=ID(unique=True, stored=True),
                      body=TEXT(vector=bool(kwargs.get("vector", index_data.get("vector", True))),
                                stored=bool(kwargs.get("stored", index_data.get("stored", True)))),
                      path=ID(unique=True, stored=True))

    def clean_index(self, graph=False, link_field='links'):
        self.reset()
        with self._index.writer(process=4, limitmb=512, multisegment=True) as writer:
            if not graph:
                # Assume we have a function that gathers the filenames of the
                # documents to be indexed
                for doc in self.collection:
                    self.add_doc(writer, doc)
            else:
                base_path = str(self.graph_path.absolute())

                path_adjlist_tmp = Path(os.path.join(base_path, 'graph.adjlist.tmp'))
                path_adjlist_tmp.touch(exist_ok=True)
                file_adjlist = path_adjlist_tmp.open(mode='w')

                for doc in self.collection:
                    title = doc.name
                    links = doc.fields.get(link_field, [])

                    if not isinstance(links, list):
                        links = [links]

                    self.add_doc(writer, doc)

                    article_title = normalize_title(title)
                    adj_graph = " ".join([normalize_title(link) for link in links])
                    file_adjlist.write(article_title + " " + adj_graph + "\n")

                file_adjlist.close()
                adj = Adjacency()
                adj.load_from_index(path_adjlist_tmp)
                adj.write_adjlist_clean(base_path)
                pr = PageRank(base_path)
                pr.load_adjlist()
                pr.load_graphml()
                pr.generate_rank()
                # path_adjlist_tmp.unlink(missing_ok=True)

    def reset(self):
        self._index = index.create_in(self.index_path, schema=self.get_schema())

        shutil.rmtree(self.graph_path)
        self.graph_path = self._resolve_graph_path()

    def incremental_index(self):
        # The set of all paths in the index
        indexed_docs = set()

        with self._index.searcher() as searcher, self._index.writer(process=4, limitmb=512) as writer:
            # Loop over the stored fields in the index
            for fields in searcher.all_stored_fields():
                doc_name = fields['name']
                indexed_docs.add(doc_name)

                if doc_name not in self.collection:
                    # This file was deleted since it was indexed
                    writer.delete_by_term('name', doc_name)

            # Loop over the files in the filesystem
            # Assume we have a function that gathers the filenames of the
            # documents to be indexed
            for doc in self.collection:
                if doc.name not in indexed_docs:
                    # This is a new file that wasn't indexed before. So index it!
                    self.add_doc(writer, doc)

    def add_doc(self, writer, doc: ADoc):
        """
        Nesta primeira abordagem, consideraremos a união de todos os campos como o conteúdo do documento.
        """
        content = ""
        for field in doc.fields.values():
            if isinstance(field, bytes):
                content += field.decode("utf-8")
            else:
                content += field
            content += "\n"

        writer.add_document(name=doc.name,
                            body=content.strip(),
                            path=self.collection.get_docpath(doc.name))