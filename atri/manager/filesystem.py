# Copyright 2020 Marcos Pontes. All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.

#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.

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

# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of MARCOS PONTES.

"""
File sytem of atri. The root of  the filesystem is based on the ATRI_ROOT constant in the
config file and on the core module.
This module provides CRUD and search functionalities to ADocs and ACols managed by the tool.
"""
import pickle
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Union, Generator, Set, AnyStr, Tuple

from atri.config import ATRI_ROOT
from atri.core import ACol, ADoc
from atri.core.constants import DEFAULTS, INDEXING, DEFAULT_METRIC_LIST
from atri.core.index.index import AIndexer
from atri.core.metrics import get_metric
from atri.core.parsing import get_parser
from atri.core.ranking import ASearcher
from atri.error import AtriError


class FileSystemError(Exception):
    """
        Handle default de erros do file system do atri
    """
    pass


class CollectionManager(object):
    """
    Interface to execute common operations inside a collection.
    """

    def __init__(self, collection: ACol, temporary_folder: Path):
        """
        Creates a new CollectionManager instance.
        :param collection: ACol instance.
        """
        self.collection = collection
        self.temporary_folder = temporary_folder

    def index(self):
        """
        Indexes the collection.
        """
        defaults = self.collection.index_defaults
        indexer = AIndexer(self.collection, **defaults)

        clean = defaults.get('clean', False)  # Incremental indexing is important here.
        graph = defaults.get('graph', False)  # Graph indexing is important for algorithms like PageRank.
        link_field = defaults.get('link_field', 'links')  # Link field is important for graph indexing.

        try:
            indexer.index(clean=clean, graph=graph, link_field=link_field)
        except Exception as e:
            raise FileSystemError(f"Index process failed. Reason: {str(e)}")

    def get_doc(self, filename: str) -> ADoc:
        """
        Returns the document with the given filename.
        :param filename: str.
        :return: ADoc instance.
        """
        try:
            return self.collection[filename.strip()]
        except KeyError:
            raise FileSystemError(f"Document {filename} not found.")

    def add_doc(self, filename: str, content: AnyStr, commit: bool = True):
        """
        Add an indexable file into the collection.
        :param filename: Filename.
        :param content: Content of the file.
        :param commit: Whether to commit the changes to the filesystem.
        """
        file_path = self.temporary_folder.joinpath(filename).absolute()

        # 1st: create temporary file
        tmp_file = Path(file_path)

        file_path.write_bytes(content)

        try:
            self.collection.add_document_from_file(tmp_file)
        except AtriError as err:
            raise FileSystemError(f"File could not be added. Reason: {str(err)}")
        finally:
            tmp_file.unlink()

        if commit:
            self.collection.commit()

    def delete_doc(self, doc: str, commit: bool = True):
        """
        Deletes a file from the collection.
        :param doc: ADoc name.
        :param commit: Whether to commit the changes to the filesystem.
        """
        if not self.collection.delete_document(doc):  # if the document was not removed
            raise FileSystemError(f"The document {doc} does not exist or cannot be deleted.")

        if commit:
            self.collection.commit()

    def all_document_locations(self, page=1) -> Tuple[List[Path], int, int]:
        """
        Returns a list of all document locations.
        :return: List of Path instances, current page and total pages.
        """
        return self.collection.document_paths(page=page)

    def index_defaults(self) -> Dict:
        """
        Returns the defaults used to index the collection.
        :return: Dict.
        """
        return self.collection.index_defaults

    def search_defaults(self) -> Dict:
        """
        Returns the defaults used to search the collection.
        :return: Dict.
        """
        return self.collection.search_defaults


class QueryManager(object):
    def __init__(self, filename: str, content: AnyStr, temporary_folder: Path):
        self.filename = filename
        self.content = content
        self.temporary_folder = temporary_folder

    def parse(self, multiquery: bool = False) -> Union[ADoc, Generator[ADoc, None, None]]:
        """
        Parse the query storage.
        :param multiquery: Whether the query is a multiquery.
        """
        try:
            if not multiquery:
                return self.__parse_file_query()
            else:
                return self.__parse_multiquery()
        except Exception as e:
            raise FileSystemError(f"Query parse failed. Reason: {str(e)}")

    def __parse_multiquery(self) -> Union[ADoc, Generator[ADoc, None, None]]:
        """
        Parse a multiquery storage.
        """
        file_path = self.temporary_folder.joinpath(self.filename).absolute()

        # 1st: create temporary file
        tmp_file = Path(file_path)

        # 2nd: write query content to temporary file
        file_path.write_bytes(self.content)

        # 3rd: parse the query
        try:
            parser = get_parser(tmp_file, True)
            queries_generator = parser(tmp_file)
        finally:
            # 3rd: remove temporary file
            tmp_file.unlink()

        return queries_generator

    def __parse_file_query(self) -> Union[ADoc, Generator[ADoc, None, None]]:
        """
        Parse the query storage considering a single file.
        :return: A single query document or a generator of query documents
        """
        file_path = self.temporary_folder.joinpath(self.filename).absolute()

        # 1st: create temporary file
        tmp_file = Path(file_path)

        # 2nd: write query content to temporary file
        file_path.write_bytes(self.content)

        # 3rd: parse the query
        try:
            atri_docs = ACol.doc_from_file(tmp_file)
        finally:
            tmp_file.unlink()

        return atri_docs

    @classmethod
    def keywords_query(cls, atri_docs: Union[ADoc, Generator[ADoc, None, None]], fields: Set[str] = None) -> str:
        """
        Get  the keywords given a Generator of query documents.
        :param atri_docs: Generator of query documents or  single query document.
        :param fields: Set of fields.
        :return:
        """
        if isinstance(atri_docs, ADoc):  # single query doc
            fields = fields if fields else atri_docs.fields.keys()
            return '\n'.join([value for field, value in atri_docs.fields.items() if field in fields])
        else:  # generator of query docs
            keywords = str()
            for atri_doc in atri_docs:
                fields = fields if fields else atri_doc.fields.keys()
                keywords += '\n'.join([value for field, value in atri_doc.fields.items() if field in fields])
            return keywords


class SearchManager(object):
    def __init__(self, collection: ACol, temporary_folder: Path):
        self.collection = collection
        self.temporary_folder = temporary_folder

    def defaults(self):
        """
        Get the default search parameters for the collection.
        :return:
        """
        return self.collection.index_defaults

    def search_defaults(self):
        """
        Get the similarities for the collection.
        :return:
        """
        return self.collection.search_defaults

    def search(self, keywords, **kwargs):
        """
        Search the collection.
        :param keywords: Keywords to search.
        :param kwargs:
        :return:
        """
        try:
            indexer = AIndexer(self.collection)
            searcher = ASearcher(indexer)
            hits = searcher.search(keywords, **kwargs)
        except Exception as e:
            raise FileSystemError(f"Search failed. Reason: {str(e)}")
        return SearchManager.__extract_hits(hits, field_name=searcher.fieldname,
                                            summarization=bool(kwargs.get('summarization', True)))

    def multiquery_search(self, query_generator, **kwargs):
        """
        Search the collection.
        :param query_generator: Generator of queries.
        :param kwargs:
        :return:
        """
        metrics = kwargs.pop('metrics') if 'metrics' in kwargs and kwargs['metrics'] \
            else DEFAULT_METRIC_LIST

        metrics_handle = {metric: SearchManager.__metric_handle(metric, **kwargs) for metric in metrics}

        evaluation = {metric: 0.0 for metric in metrics}
        ratio = 0.0

        for qry, qrel in query_generator:
            try:
                indexer = AIndexer(self.collection)
                searcher = ASearcher(indexer)
                hits = searcher.search(qry, **kwargs)
            except Exception as e:
                raise FileSystemError(f"Search failed. Reason: {str(e)}")
            truth = (lambda x: qrel[x]) if isinstance(qrel, dict) else (lambda x: int(x in qrel))
            response = SearchManager.__extract_hits(hits, field_name=searcher.fieldname,
                                                    summarization=False)

            hits_evaluated = [truth(name) for name, _ in response.items()]
            for metric in metrics:
                fn, params = metrics_handle[metric]
                params['nrel'] = len(qrel)  # some query-based stats
                evaluation[metric] += fn(hits_evaluated, **params)
            ratio += 1

        for metric in metrics:
            evaluation[metric] /= ratio

        return evaluation

    @classmethod
    def evaluate(cls, metric, ground_truth, **kwargs):
        """
        Calculates  relevance metric given a ground_truth graded ranking.
        """
        metric_func, kwargs = cls.__metric_handle(metric, **kwargs)
        return metric_func(ground_truth, **kwargs)

    @classmethod
    def __metric_handle(cls, metric: str, **kwargs):
        """
        Get the metric handle function.
        :param metric:
        :param kwargs:
        :return:
        """

        def split_limit(metric):
            if "@" in metric:
                metric_and_k = metric.split("@")
                return metric_and_k[0], int(metric_and_k[1])
            return metric, None

        metric_name, k = split_limit(metric)
        if k:
            kwargs['limit'] = k

        return get_metric(metric_name), kwargs

    @classmethod
    def __extract_hits(cls, hits, field_name, summarization=False):
        """
        Extract the hits from the search.
        """
        hits_json = {}
        for hit in hits:
            if summarization:
                summary = cls.__extract_summary(hit, field_name)
                hits_json[hit['name']] = {"score": hit.score, "position": hit.pos + 1, "summary": summary}
            else:
                hits_json[hit['name']] = {"score": hit.score, "position": hit.pos + 1}
        return hits_json

    @classmethod
    def __extract_summary(cls, hit, field_name):
        """
        Extract the summary of the document.
        """
        try:
            doc_path = Path(hit['path'])
            with doc_path.open(mode="rb") as io:
                atri_doc: ADoc = pickle.load(io)
            return hit.highlights(field_name, atri_doc.fields[field_name])
        except KeyError as e:
            raise FileSystemError(f"Search process failed ({str(e)})")


class FileSystem:
    __instance = None

    def __init__(self, root: str = ATRI_ROOT):
        """
        Construct a FileSystem singleton instance.
        :param root: Root path of the FileSystem. Default =  ATRI_ROOT.
        """
        if FileSystem.__instance is not None:
            raise FileSystemError("FileSystem is a singleton class")

        self.root: Path = Path(root)
        self.collections: Dict[str, ACol] = {}

        self._temporary = self.root.joinpath("_temp")  # Folder to store temporary files
        self._initialize_filesystem()

    def _initialize_filesystem(self):
        """
        Initializes  directories and metadata.
        """
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            self._temporary.mkdir(parents=True, exist_ok=True)
            self._update_collections()
        except FileExistsError:
            raise FileSystemError("Could not create root directory")

    def _update_collections(self):
        """
            Updates all in-memory collections in the filesystem and stores them in the collections' dictionary.
        """
        folders = self.all_folders()
        for fold in folders:
            try:
                atri_col = ACol.load(fold)
                self.collections[atri_col.name] = atri_col
            except AtriError:  # ignore folders that aren't collections
                continue

    def all_folders(self) -> List[Path]:
        """
        Returns all folders in the filesystem.
        :return: List of Paths.
        """
        return [f for f in self.root.iterdir() if f.is_dir() and f.absolute() != self._temporary.absolute()]

    @classmethod
    def get(cls) -> 'FileSystem':
        """
        Create  an instance of FileSystem or return the existing one.
        :return: An instance of FileSystem.
        """
        if cls.__instance is None:
            cls.__instance = FileSystem()
        return cls.__instance

    def get_all(self) -> List[ACol]:
        """
        Returns all collections in the filesystem.
        :return: List of ACol instances.
        """
        return list(self.collections.values())

    def load(self):
        """
        Load all collections on the file system into memory.
        """
        self._update_collections()

    def save(self):
        """
        Save all collections in memory to the file system.
        """
        for col in self.collections.values():
            col.commit()

    def add(self, name: str, description: str):
        """
        Adds a collection in the filesystem.
        :param name: Collection name.
        :param description: Collection description.
        """
        collection_path = self.root.joinpath(name)
        if name in self.collections or collection_path.exists():
            raise FileSystemError("Collection already exists")

        self.collections[name] = ACol(
            name, description, collection_path, index_defaults=INDEXING, search_defaults=DEFAULTS
        )

    def remove(self, name: str):
        """
        Removes a collection from the filesystem.
        :param name: Collection name.
        """
        if name not in self.collections:
            raise FileSystemError("Collection not found")

        collection = self.collections.pop(name)
        ACol.delete(collection)  # call del on the ACol instance

    def rename(self, name: str, new_name: str, commit: bool = True):
        """
        Renames a collection.
        :param name: Collection name.
        :param new_name: New collection name.
        :param commit: Whether to commit the changes to the filesystem.
        """
        if name not in self.collections:
            raise FileSystemError("Collection not found")
        if new_name in self.collections:
            raise FileSystemError("Collection already exists")

        collection = self.collections.pop(name)
        collection.name = new_name
        collection.path = collection.path.parent.joinpath(new_name)

        self.collections[new_name] = collection

        if commit:
            collection.commit()

    def update_collection(self, collection_name: str, **kwargs):
        """
        Updates a collection.
        :param collection_name: Collection name.
        :param kwargs: Keyword arguments to update the collection.
        """
        if collection_name not in self.collections:
            raise FileSystemError("Collection not found")

        collection = self.collections[collection_name]

        if 'name' in kwargs and kwargs['name']:
            self.rename(collection_name, kwargs['name'], False)  # avoid double committing

        if 'description' in kwargs and kwargs['description']:
            collection.description = kwargs['description']

        if 'index_defaults' in kwargs and kwargs['index_defaults']:
            collection.index_defaults = kwargs['index_defaults']

        if 'search_defaults' in kwargs and kwargs['search_defaults']:
            collection.search_defaults = kwargs['search_defaults']

        collection.commit()

    def collection_manager(self, name: str) -> CollectionManager:
        """
        Returns a collection manager for the given collection.
        :param name: Collection name.
        :return: CollectionManager instance.
        """
        if name not in self.collections:
            raise FileSystemError("Collection not found")

        return CollectionManager(self.collections[name], self._temporary)

    def query_manager(self, filename: str, content: AnyStr) -> QueryManager:
        """
        Returns a query manager for the given file.
        :param filename: Filename.
        :param content: Content of the file.
        :return: QueryManager instance.
        """
        return QueryManager(filename, content, self._temporary)

    def search_manager(self, name: str) -> SearchManager:
        """
        Returns a search manager for the given collection.
        :param name: Collection name.
        :return: SearchManager instance.
        """
        if name not in self.collections:
            raise FileSystemError("Collection not found")

        return SearchManager(self.collections[name], self._temporary)

    @staticmethod
    @lru_cache
    def file_info(path: Path):
        """
        Returns information about a file.
        :param path: Path to the file.
        :return: JSON with file info.
        """
        name = path.with_suffix('').name
        return \
            {
                "name": name,
                "size": path.stat().st_size / (1024 * 1024),
            }
