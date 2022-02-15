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

def recall(ground_truth_permutated, **kwargs):
    """
    Calculate the recall at k
    @param ground_truth_permutated: Ranked results with its judgements.
    """
    rank = ground_truth_permutated
    limit = kwargs.get('limit', len(ground_truth_permutated))
    nrel = float(kwargs.get('nrel', len(ground_truth_permutated)))

    if nrel > len(ground_truth_permutated):
        nrel = len(ground_truth_permutated)

    if len(rank) < limit:
        # 0-padding
        rank += [0] * (limit - len(rank))

    recall = 0.0
    for hit in rank[:limit + 1]:
        if hit > 0:
            recall += hit
    return recall / nrel if nrel > 0 else 0.0


def precision(ground_truth_permutated, **kwargs):
    """
    Calculate the precision at k
    @param ground_truth_permutated: Ranked results with its judgements.
    """
    rank = ground_truth_permutated
    limit = kwargs.get('limit', len(ground_truth_permutated))

    if len(rank) < limit:
        # 0-padding
        rank += [0] * (limit - len(rank))

    prec = 0.0
    for hit in rank[:limit + 1]:
        if hit > 0:
            prec += 1.0
    return prec / limit if limit > 0 else 0.0


def f1(ground_truth_permutated, **kwargs):
    """
    Calculate the f1 score at k
    @param ground_truth_permutated: Ranked results with its judgements.
    """
    prec = precision(ground_truth_permutated, **kwargs)
    rec = recall(ground_truth_permutated, **kwargs)

    return 2 * (prec * rec / (prec + rec)) if prec + rec > 0 else 0.0
