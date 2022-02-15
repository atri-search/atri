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
Search module of mdocs
"""
from whoosh.qparser.syntax import AndGroup, OrGroup, NotGroup
from whoosh.qparser import QueryParser
from whoosh.searching import Results

from atri.core.index.index import AIndexer
from atri.core.ranking.aggregation import BordaCount, MarkovChain
from atri.error import AtriError
from atri.core.constants import DEFAULTS, SIMILARITIES


class ASearcher(object):
    def __init__(self, index: AIndexer):
        self._index = index
        self._searcher = None
        self._defaults = None
        self._similarities = None
        self._weighting_cache = {}
        self.__dict__.update(self._get_default_attributes())

    def __del__(self):
        if self._searcher and not self._searcher.is_closed:
            self._searcher.close()

    def search(self, keywords, **kwargs):
        """
        kwargs:
        - fieldname
        - query
        - similarity
        - similarity specific attributes
        - plain
        @warning: remeber to close the searcher after use it again.
        """
        import re

        #  plain flag: if True, the query is not parsed
        if kwargs.get("plain", False):
            keywords = ' '.join(
                re.sub(r'[\W]', '', kw) for kw in keywords.split(' '))

            keywords = keywords.replace('AND', '')
            keywords = keywords.replace('OR', '')
            keywords = keywords.replace('NOT', '')

        try:
            # fieldname for search
            fieldname = kwargs.get("fieldname", self.fieldname)
            self.__dict__.update({"fieldname": fieldname})

            # define the main query_group of search
            query_group = self._get_query_group(
                kwargs.get('query', self._defaults['query']))

            # define similarity function
            kwargs['similarity'] = kwargs.get('similarity',
                                              self._defaults['similarity'])

            # define the query
            term_query = self._get_query(keywords, fieldname, query_group)

            # 1st verify if similarity  is a list
            sim = kwargs.pop("similarity", self._defaults["similarity"])
            if isinstance(sim, list):
                # aggregation search
                results = []
                aggr = kwargs.get("aggregation", self._defaults["aggregation"])
                aggregator = self._get_aggregation_method(aggr)

                for w in self._weighting_generator(sim):
                    results.append(self.single_weighting_search(
                        term_query, w, **kwargs
                    ))

                return self._rank_aggregation(results, aggregator, term_query)

            else:
                # single weighting search
                weighting = self._get_weighting(sim, **kwargs)
                return self.single_weighting_search(term_query, weighting, **kwargs)

        except Exception as er:
            raise AtriError("Unknown error: {}".format(str(er)))

    def single_weighting_search(self, term_query, weighting, **kwargs):
        """
        @brief Perform a search considering a single weighting schema.
        @details kwargs
            : query group
            : similarity function
            : similarity parameters
            : plain query 
        """
        try:

            self._searcher = self._index.searcher(weighting=weighting)
            results = self._searcher.search(term_query,
                                            limit=kwargs.get('limit', self._defaults['limit'])
                                            )

            return results

        except Exception as er:
            raise AtriError("Unknown error: {}".format(str(er)))

    def close(self):
        del self._searcher

    @property
    def fieldname(self):
        try:
            return self.__dict__.get('fieldname', self._defaults['fieldname'])
        except KeyError:
            raise AtriError(
                "Any fieldname found. Check the machup.yml defaults.")

    def _get_default_attributes(self):
        attributes = {'_defaults': DEFAULTS, '_similarities': SIMILARITIES}
        return attributes

    def _get_query_group(self, query_group):
        """
        Valid options are: AND, OR and NOT
        """
        query_group = query_group.lower().strip()
        if query_group == 'and':
            return AndGroup
        elif query_group == 'or':
            return OrGroup
        elif query_group == 'not':
            return NotGroup
        raise AtriError(
            "QueryGroup {} not supported by Atri.".format(query_group))

    def _get_aggregation_method(self, aggr):
        if aggr == 'borda_count':
            return BordaCount()
        elif aggr == 'markov_chain':
            return MarkovChain()
        raise ValueError(f"Invalid aggregation method: {aggr}")

    def _get_query(self, keywords, fieldname, query_group):
        qp = QueryParser(fieldname,
                         self._index.get_schema(),
                         group=query_group)
        qry = qp.parse(keywords)
        return qry

    def _rank_aggregation(self, results, aggregator, qry):
        rankings = [[hit.docnum for hit in rank] for rank in results]
        new_rank = aggregator.rank_by_id(rankings)
        return Results(self._searcher, qry, new_rank)

    def _get_weighting(self, similarity, **kwargs):
        s = self._get_similarity_one_item(similarity, **kwargs)
        self._resolve_specific_weighting_metadata(s)
        return s

    def _get_similarity_one_item(self, similarity, **kwargs):
        similarity = similarity.lower().strip()

        if similarity in self._weighting_cache:
            return self._weighting_cache[similarity]

        if similarity in self._similarities.keys():
            import importlib

            sim = self._similarities[similarity]
            module_name, class_name = sim['ref'].rsplit(".", 1)
            clazz = getattr(importlib.import_module(module_name), class_name)

            # verifying required attributes
            clazz_kwargs = {}
            if 'required' in sim and 'defaults' in sim:
                for attr in sim['required']:
                    try:

                        attr = attr.lower().strip()
                        attr_value = kwargs.get(attr, sim['defaults'][attr])
                        try:
                            numeric_attr = float(attr_value)
                            clazz_kwargs[attr] = numeric_attr
                        except ValueError:
                            clazz_kwargs[attr] = attr_value

                    except KeyError:
                        raise AtriError(
                            "Defaults value not provided for {} attribute.".
                                format(attr))

            self._weighting_cache[similarity] = clazz(**clazz_kwargs)
            return self._weighting_cache[similarity]

        raise AtriError(
            "Similarity {} not supported by atri.".format(similarity))

    def _resolve_specific_weighting_metadata(self, weighting):
        from atri.core.similarities.pagerank import PageRankBM25
        if isinstance(weighting, PageRankBM25):
            weighting.set_pagerank(self._index.graph_path)

    def _weighting_generator(self, sim):
        """
        @brief Generate a list of weighting schemas.
        @details sim is a list of weighting schemas.
        """
        for w in sim:
            # check if w is a dict
            try:
                # key : similarity, values: parameters
                # pick the 1st key
                similarity = list(w.keys())[0]
                # get the value
                params = w[similarity]
                yield self._get_weighting(similarity, **params)
            except Exception as er:
                raise AtriError("Invalid weighting schema: {}".format(str(er)))
