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

from abc import ABC, abstractmethod
import numpy as np
import pandas as pd

"""
Ranking aggregation module
"""


class Aggregator(ABC):

    @abstractmethod
    def rank_by_id(self, rankings):
        """
        List of strings for doc ids.
        :param rankings: List of rankings by docid str
        :return: final ranking list of docids (list)
        """
        pass

    @staticmethod
    def get_unique_doc_ids(rankings):
        docs = set()
        for rank in rankings:
            for docid in rank:
                docs.add(docid)
        return docs


class BordaCount(Aggregator):
    def __init__(self):
        """
        BordaCount initializer.
        """
        self.num_documents = 0
        self.num_rankings = 0

    def _rank_score(self, rank):
        """
        Generate BordaCount scores for a single rank.
        E.g. [1, 3, 5, 2, 4]  => [4, 1, 3, 0, 2]
        :param rank: rank of interest
        :param n_rows: number of rows in the rank
        :return:
        """
        n_rows = self.num_documents
        error = 0
        borda_rank = np.zeros(n_rows, dtype=np.int64)

        for i in range(n_rows):
            if rank[i] - 1 < 0:
                error = 1
            else:
                borda_rank[rank[i] - 1] = n_rows - (i + 1)

        if error == 1:
            raise ValueError("Error in BordaCount::rank : Doc id's must be higher than 1")

        return borda_rank

    def _fit(self, rankings):
        """
                Apply BordaCount
                :param rankings: all rankings
        """
        ndocs = self.num_documents
        nranks = self.num_rankings
        scores = np.zeros((nranks, ndocs), dtype=np.int64)
        data = np.zeros(ndocs, dtype=np.int64)

        for i in range(nranks):
            scores[i, :] = self._rank_score(rankings[i, :])

        data = scores.sum(axis=0)

        return (data.argsort() + 1)[::-1], data

    def _rank(self, rankings):
        """
        Main method of BordaCount process. Here, all rankings are compared and a new ranking is produced based on they.
        Important, each rank must be considered as ranking of ID's from 1 to Number of documents to be ranked.
        :param rankings: all rankings
        :return: final ranking tuple (list of ids (ndarray), list of scores indexed by id - 1)
        """

        self.num_rankings, self.num_documents = rankings.shape
        idx, score = self._fit(rankings)
        return np.asarray(idx), np.asarray(score)

    def rank_by_id(self, rankings):
        """
        List of strings for doc ids.
        :param rankings: List of rankings by docid str
        :return: final ranking list of docids (list) and scores
        """
        docs = Aggregator.get_unique_doc_ids(rankings)

        nrank = len(rankings)
        ndoc = len(docs)
        map_doc_ids = dict()
        map_ids_doc = dict()

        ndranks = np.zeros((nrank, ndoc), dtype=np.int64)

        if len(rankings) > 0:
            for i, docid in enumerate(docs, start=1):
                map_doc_ids[docid] = i
                map_ids_doc[i] = docid

            set_doc_ids = set(map_doc_ids.values())

            for i, rank in enumerate(rankings):
                # padding:
                rank_ids = [map_doc_ids[docid] for docid in rank]

                set_rank_ids = set(rank_ids)
                padding = list(set_doc_ids - set_rank_ids)  # padding arbitario

                ndranks[i, :] = np.asarray(rank_ids + padding)

            idx, scores = self._rank(ndranks)

            return [(float(scores[idx[i] - 1]), map_ids_doc[idx[i]]) for i in range(ndoc)]

        raise ValueError("Error in BordaCount::rank : You must provide some ranking")


# Copyright: all rights to Ayan Kumar Saha
class MarkovChain(Aggregator):

    def get_matrix_shape(self, df):

        """Returns shape(rows,cols) of a dataframe
        Args:
            df (pandas.core.Dataframe): pandas dataframe objects
        Returns:
            int,int: shape in the form of rows,cols
        """

        rows = len(df.index)
        cols = len(df.columns)

        return rows, cols

    def get_partial_transition_matrix(self, df, algo, items, lists):

        """Returns the partial transition matrix from the dataframe containing different ranks
        """

        resultant_matrix = list()

        for i in range(items):
            for j in range(items):

                result = len(df.columns[df.iloc[i] < df.iloc[j]])

                if result == 0 and i == j:
                    val = -1
                elif result > (lists / 2):
                    val = 0
                else:
                    if algo == 'mc4':
                        val = 1
                    else:
                        val = (lists - result) / lists

                resultant_matrix.append(val)

        return np.array(resultant_matrix).reshape(items, items)

    def get_normalized_transition_matrix(self, p_matrix, items):

        """Returns the normalized transition matrix from the partial transition matrix
        Args:
            p_matrix (numpy.ndarray): partial transition matrix
            items (int): number of items
        Returns:
            numpy.ndarray: normalized transition matrix
        """

        p_matrix = p_matrix / items

        for i in range(items):
            for j in range(items):
                if i == j:
                    result1 = sum(p_matrix[i, i + 1:items])
                    result2 = sum(p_matrix[i, 0:j])
                    result = result1 + result2

                    p_matrix[i, j] = 1 - result

        return p_matrix

    def get_ergodic_transition_matrix(self, n_matrix, items, erg_number):

        """Returns the ergotic transition matrix from normalized transition matrix
        Args:
            n_matrix (numpy.ndarray): normalized transition matrix
            items (int): number of items
            erg_number (float): small, positive ergotic number
        Returns:
            numpy.ndarray: ergodic transition matrix
        """

        return (n_matrix * (1 - erg_number)) + (erg_number / items)

    def get_initial_distribution_matrix(self, items):

        """Returns initial distribution matrix
        Args:
            items (int): number of items
        Returns:
            numpy.ndarray: initial distribution matrix
        """

        return np.repeat((1 / items), items)

    def get_stationary_distribution_matrix(self, state_matrix, transition_matrix, precision, iterations):

        """Returns stationary distribution matrix
        Args:
            state_matrix (numpy.ndarray): initial distribution matrix
            transition_matrix (numpy.ndarray): final transition matrix or ergodic transition matrix
            precision (float): acceptable error margin for convergence, default is 1e-07
            iterations (int): number of iterations to reach stationary distribution, default is 200
        Returns:
            numpy.ndarray: stationary distribution matrix
        """

        counter = 1

        while counter <= iterations:

            current_state_matrix = state_matrix

            new_state_matrix = state_matrix.dot(transition_matrix)

            error = new_state_matrix - current_state_matrix

            if (np.abs(error) < precision).all():
                break

            state_matrix = new_state_matrix

            counter += 1

        return state_matrix

    def get_aggregated_ranks(self, matrix):

        """Return the final aggregated ranks based on the stationary distribution matrix
        Args:
            matrix (numpy.ndarray): stationary distribution matrix
        Returns:
            list: final aggregated ranks
        """

        a = {}
        rank = 1

        for num in sorted(matrix, reverse=True):
            if num not in a:
                a[num] = rank
                rank = rank + 1

        final_ranks = [a[i] for i in matrix]

        return final_ranks

    def get_mapped_final_ranks(self, df, final_ranks, index_col):

        ranks = dict()

        if index_col != None:

            for item, rank in zip(df.index, final_ranks):
                ranks[item] = rank

        else:

            items = np.arange(0, len(df.index) + 1)

            for item, rank in zip(items, final_ranks):
                ranks[item] = rank

        return ranks

    def mc4_aggregator(self, df, algo='mc4', index_col=None, precision=0.0000001,
                       iterations=200, erg_number=0.15):

        """Performs aggregation on different ranks using Markov Chain Type 4 Rank Aggeregation algorithm and returns
         the aggregated ranks
        """

        if algo not in ['mc4', 'mct']:
            raise Exception(f"Invalid ranking algorithm '{algo}'")

        rows, cols = self.get_matrix_shape(df)

        partial_transition_matrix = self.get_partial_transition_matrix(df, algo, rows, cols)

        normalized_transition_matrix = self.get_normalized_transition_matrix(partial_transition_matrix, rows)

        ergodic_transition_matrix = self.get_ergodic_transition_matrix(normalized_transition_matrix, rows, erg_number)

        initial_distribution_matrix = self.get_initial_distribution_matrix(rows)

        stationary_distribution_matrix = self.get_stationary_distribution_matrix(initial_distribution_matrix,
                                                                                 ergodic_transition_matrix, precision,
                                                                                 iterations)

        final_ranks = self.get_aggregated_ranks(stationary_distribution_matrix)

        mapped_final_ranks = self.get_mapped_final_ranks(df, final_ranks, index_col)

        return mapped_final_ranks

    def rank_by_id(self, rankings):
        """
                List of strings for doc ids.
                :param rankings: List of rankings by docid str
                :return: final ranking list of docids (list) and scores
                """

        docs = Aggregator.get_unique_doc_ids(rankings)

        nrank = len(rankings)
        ndoc = len(docs)

        map_doc_ids = dict()
        map_ids_doc = dict()

        ndranks = np.zeros((nrank, ndoc), dtype=np.int64)
        ndpos = np.zeros((ndoc, nrank), dtype=np.int64)

        if len(rankings) > 0:
            for i, docid in enumerate(docs, start=1):
                map_doc_ids[docid] = i
                map_ids_doc[i] = docid

            paddings = []
            for rank in rankings:
                paddings.append(list(docs - set(rank)))

            for i, rank in enumerate(rankings):
                ndranks[i, :] = np.asarray(
                    [map_doc_ids[docid] for docid in rank] + [map_doc_ids[docid] for docid in paddings[i]])

            for i, rank in enumerate(ndranks):
                for pos, doc in enumerate(rank):
                    ndpos[doc - 1, i] = pos + 1

            mc4 = self.mc4_aggregator(pd.DataFrame(ndpos))
            size = ndpos.shape[0]
            rank = [(float(size - rank), map_ids_doc[item + 1]) for item, rank in mc4.items()]
            rank.sort(key=lambda x: -x[0])
            return rank

        raise ValueError("Error in MarkovChain::rank : You must provide some ranking")


if __name__ == '__main__':
    bc = BordaCount()
    rankings = [['A', 'C', 'B'], ['B', 'A', 'C'], ['B', 'C', 'A'], ['A', 'B', 'C']]
    aggr = bc.rank_by_id(rankings)
    print(aggr)

    mc = MarkovChain()
    aggr = mc.rank_by_id(rankings)
    print(aggr)
