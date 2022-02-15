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
Parsing  from different well known small  collections to AtriDocs.
"""

import itertools
import json
from multiprocessing import Pool
from typing import List
from pathlib import Path
from atri.core.primitives.document import ADoc


class DOM(object):
    def __init__(self, f: Path):
        self.ignore = None
        self.chunks = None
        self.corpus = None
        self._extract_corpus(f)
        self.__files = list()

    def __call__(self, *args, **kwargs):
        self.__all_files(self.corpus)
        chunks = self.chunks if "chunks" in self.__dict__ else 4

        for chunk in self._chunks(self.__files, chunks):
            # chunk of files
            for atri_doc in self.next_doc(chunk):
                yield atri_doc

    def next_doc(self, chunk):
        with Pool(processes=4) as p:
            atri_docs = list(p.map(self._resolve, chunk))
        return itertools.chain(*atri_docs)

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        raise NotImplementedError("Parser not found.")

    def _extract_corpus(self, config):
        raise NotImplementedError("Invalid configuration")

    @staticmethod
    def _chunks(iterable, chunks):
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, chunks))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, chunks))

    def __all_files(self, path):

        for subdir in path.iterdir():
            if subdir.name in self.ignore:
                continue
            if subdir.is_dir():
                return self.__all_files(subdir)

            self.__files.append(subdir)


class LISA(DOM):
    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)

        try:
            self.corpus = Path(data['path'])
            self.ignore = data['ignore']
        except KeyError:
            raise ValueError("Invalid configuration")

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        atri_docs = []
        documents = set()
        with p.open(mode='r') as f:  # Reading file

            title = ""
            buffer = ""
            docid = ""

            separator = "********************************************"

            for line in f.readlines():
                buffer += line

                if separator in line:
                    body = buffer.strip()

                    if docid and body and title:
                        atri_docs.append(ADoc(docid, title=title, body=body))

                    docid = ""
                    title = ""
                    buffer = ""

                elif line.startswith("Document"):
                    docid = line.split("Document")[-1].strip()

                    if docid in documents:
                        copy = 1
                        while docid not in documents:
                            docid = docid + f" ({copy})"
                            copy += 1

                    documents.add(docid)
                    buffer = ""

                elif not line.strip():
                    title = buffer
                    buffer = ""

        return atri_docs


class LISA_QREL(object):
    def __init__(self, f: Path):

        self._extract_corpus(f)

    def __call__(self, *args, **kwargs):
        for qry, rel in zip(self._next_query(), self._next_relevance()):
            qid, body = qry
            qid_rel, docs = rel
            if qid != qid_rel:
                raise RuntimeError("Unexpected error raised by LISA_QREL()")
            yield body, docs  # emito a consulta e o gabarito para testar

    def _next_query(self):
        for q in self.queries:
            with q.open(mode='r') as f:
                qid = 1
                body = ""
                for line in f:
                    try:
                        qnum = int(line.strip())
                        if qnum != qid:
                            yield qid, body
                            qid = qnum
                            body = ""
                    except ValueError:
                        body += line.strip() + "\n"

    def _next_relevance(self):
        for rel in self.relevance:
            with rel.open(mode='r') as f:
                qid = 1
                rel = []
                for line in f:
                    if line.startswith('Query'):
                        qnum = int(line.split()[-1])
                        if qnum != qid:
                            yield qid, set(rel)
                            qid = qnum
                            rel = []
                    elif not line.strip() or line.strip().endswith('Refs:'):
                        continue
                    else:
                        if line.strip().endswith('-1'):
                            rel += line.strip().split(' ')[:-1]
                        else:
                            rel += line.strip().split(' ')

    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)
        try:
            self.queries = [Path(q) for q in data['queries']]
            self.relevance = [Path(rel) for rel in data['relevance']]
        except KeyError:
            raise ValueError("Invalid qrel for LISA")


class NPL(DOM):
    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)

        try:
            self.corpus = Path(data['path'])
            self.ignore = data['ignore']
        except KeyError:
            raise ValueError("Invalid configuration")

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        atri_docs = []
        documents = set()
        with p.open(mode='r') as f:  # Reading file

            buffer = ""
            docid = ""

            separator = "   /"

            for line in f.readlines():
                buffer += line

                if separator in line:
                    body = buffer.strip()

                    if docid and body:
                        atri_docs.append(ADoc(docid, body=body))

                    docid = ""
                    buffer = ""

                else:
                    if line.strip().isdigit():
                        docid = line.strip()
                        if docid in documents:
                            copy = 1
                            while docid not in documents:
                                docid = docid + f" ({copy})"
                                copy += 1
                        documents.add(str(docid))
                        buffer = ""
        return atri_docs


class NPL_QREL(object):
    def __init__(self, f: Path):

        self._extract_corpus(f)

    def __call__(self, *args, **kwargs):
        for qry, rel in zip(self._next_query(), self._next_relevance()):
            qid, body = qry
            qid_rel, docs = rel
            if qid != qid_rel:
                raise RuntimeError(
                    f"Unexpected error raised by NPL_QREL() {qid} != {qid_rel}"
                )
            yield body, docs  # emito a consulta e o gabarito para testar

    def _next_query(self):
        for q in self.queries:
            with q.open(mode='r') as f:
                qid = 1
                body = ""
                for line in f:
                    if line.strip().isdigit():
                        qnum = int(line.strip())
                        if qnum != qid:
                            yield qid, body
                            qid = qnum
                            body = ""
                    else:
                        if "/" not in line:
                            body += line.strip() + "\n"

    def _next_relevance(self):
        for rel in self.relevance:
            with rel.open(mode='r') as f:
                qid = 1
                rel = []
                for line in f:
                    if line.strip().isdigit():
                        qnum = int(line.split()[-1])
                        if qnum == qid + 1:
                            yield qid, set(rel)
                            qid = qnum
                            rel = []
                        elif qnum != 1:
                            rel += line.strip()
                    elif '/' in line.strip():
                        continue
                    else:
                        rel += [
                            r for r in filter(lambda x: bool(x),
                                              line.strip().split(' '))
                        ]

    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)
        try:
            self.queries = [Path(q) for q in data['queries']]
            self.relevance = [Path(rel) for rel in data['relevance']]
        except KeyError:
            raise ValueError("Invalid qrel for LISA")


class CF(DOM):
    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)

        try:
            self.corpus = Path(data['path'])
            self.ignore = data['ignore']
        except KeyError:
            raise ValueError("Invalid configuration")

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        atri_docs = []
        with p.open(mode='r') as f:  # Reading file

            #   PN	PAPER NUMBER
            #   RN	RECORD NUMBER
            #   AN    MEDLINE ACESSION NUMBER
            #   AU    AUTHOR(S)
            #   TI    TITLE
            #   SO    SOURCE
            #   MJ    MAJOR SUBJECTS
            #   MN    MINOR SUBJECTS
            #  AB/EX  ABSTRACT/EXTRACT
            #   RF 	REFERENCES
            #   CT	CITATIONS

            fields = {"pn", "rn", "an", "au", "ti", "so", "mj", "mn", "ab", "rf", "ct"}

            buffers = {f: "" for f in fields}
            current_field = "pn"

            for line in f.readlines():
                two_start_character = line[:2]
                if two_start_character == "PN":
                    current_field = "pn"
                elif two_start_character == "RN":
                    current_field = "rn"
                elif two_start_character == "AN":
                    current_field = "an"
                elif two_start_character == "AU":
                    current_field = "au"
                elif two_start_character == "TI":
                    current_field = "ti"
                elif two_start_character == "SO":
                    current_field = "so"
                elif two_start_character == "MJ":
                    current_field = "mj"
                elif two_start_character == "MN":
                    current_field = "mn"
                elif two_start_character == "AB" or two_start_character == "EX":
                    current_field = "ab"
                elif two_start_character == "RF":
                    current_field = "rf"
                elif two_start_character == "CT":
                    current_field = "ct"

                if line[:2].lower() in fields:
                    line = line[2:].strip()

                line = line.strip()

                if line:
                    buffers[current_field] += line + "\n"

                if not line and buffers["rn"].strip().isdigit():
                    atri_docs.append(
                        ADoc(buffers.pop("rn").strip(),
                             body='\n'.join([
                                 buffers[f]
                                 for f in ["au", "ti", "mj", "mn", "ab"]
                             ]),
                             **buffers))
                    current_field = "pn"
                    buffers = {f: "" for f in fields}

            if buffers["pn"].strip() and buffers["rn"].strip().isdigit():
                atri_docs.append(
                    ADoc(buffers.pop("rn").strip(),
                         body='\n'.join([
                             buffers[f]
                             for f in ["au", "ti", "mj", "mn", "ab"]
                         ]),
                         **buffers))
        return atri_docs


class CF_QREL(object):
    def __init__(self, f: Path):

        self._extract_corpus(f)

    def __call__(self, *args, **kwargs):
        for body, rel in self._next_query():
            yield body, rel  # emito a consulta e o gabarito para testar

    def _next_query(self):
        fields = {"qn", "qu", "nr", "rd"}
        for q in self.queries:
            with q.open(mode='r') as f:
                buffers = {f: "" for f in fields}
                current_field = "qn"

                for line in f.readlines():
                    two_start_character = line[:2]
                    if two_start_character == "QN":
                        current_field = "qn"
                    elif two_start_character == "QU":
                        current_field = "qu"
                    elif two_start_character == "NR":
                        current_field = "nr"
                    elif two_start_character == "RD":
                        current_field = "rd"

                    if line[:2].lower() in fields:
                        line = line[2:].strip()

                    if line.strip():
                        buffers[current_field] += line.strip()
                        if current_field != "qn":
                            buffers[current_field] += " "

                    if not line.strip() and buffers["qn"].isdigit():
                        rel = set([
                            r.zfill(5)
                            for r in filter(lambda x: bool(x),
                                            buffers["rd"].strip().split(" "))
                        ][::2])
                        yield buffers["qu"], rel
                        current_field = "pn"
                        buffers = {f: "" for f in fields}

                if not line.strip() and buffers["qn"].isdigit():
                    rel = set(
                        r.zfill(5)
                        for r in filter(lambda x: bool(x),
                                        buffers["rd"].strip().split(" ")))
                    yield buffers["qu"], set(
                        filter(lambda x: x, buffers["rd"].strip().split(" ")))
                    current_field = "pn"
                    buffers = {f: "" for f in fields}

    def _extract_corpus(self, config: Path):
        with config.open(mode='r', encoding='utf-8') as f:
            data = json.load(f)

        try:

            self.queries = [Path(q) for q in data['queries']]

        except KeyError:
            raise ValueError("Invalid qrel for CF")
