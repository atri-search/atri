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
TREC document parsing implementation.
"""

import re
import itertools
from multiprocessing import Pool
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup

from atri.core.primitives import ADoc


class TREC(object):
    """
    Classe base de parsers TREC
    """

    def __init__(self, name: str, corpus: Path, **kwargs):
        self.name = name
        self.corpus = corpus
        self.__files = list()
        self.__chunksize = 0
        self.__dict__.update(**kwargs)

    def __call__(self, *args, **kwargs):
        self.__all_files(self.corpus)
        chunks = self.chunks if "chunks" in self.__dict__ else 4

        for chunk in self._chunks(self.__files, chunks):
            # chunk of files
            for atri_doc in self.trec_doc(chunk):
                yield atri_doc

    def trec_doc(self, chunk):
        with Pool(processes=4) as p:
            atri_docs = list(p.map(self._resolve, chunk))
        return itertools.chain(*atri_docs)

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        raise NotImplementedError("TREC parser not found.")

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


class FR94(TREC):

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:

        with p.open(mode='r') as f:  # Reading file
            soup = BeautifulSoup(f, 'lxml')

        atri_docs = []
        for doc in soup.find_all("doc"):
            doc_no = doc.find("docno").text.strip()
            doc_parent = doc.find("parent").text.strip()
            doc_text = re.sub('&blank;', ' ', doc.find("text").text.strip())

            atri_doc = ADoc(doc_no, body=doc_text, parent=doc_parent)

            # optional
            for extra in ["usdept", "agency", "usbureau", "doctitle", "address", "further", "summary",
                          "action", "signer", "signerjob", "signjob", "supplem", "billing", "frfiling",
                          "date", "rindock", "table", "footnote", "footcite", "footname"]:
                FR94.__add_extra_field(doc, atri_doc, extra)

            atri_docs.append(atri_doc)
        return atri_docs

    @staticmethod
    def __add_extra_field(soup, atri_doc, field):
        fval = soup.find(field)
        if fval:
            atri_doc.add(field, fval.text.strip())


class FT(TREC):

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        with p.open(mode='r') as f:  # Reading file
            soup = BeautifulSoup(f, 'lxml')

        atri_docs = []
        for doc in soup.find_all("doc"):
            doc_no = doc.find("docno").text.strip()
            headline = doc.find("headline").text.strip()
            doc_text = re.sub('&blank;', ' ', doc.find("text").text.strip())

            atri_doc = ADoc(doc_no, body=doc_text, headline=headline)

            # optional
            for extra in ["xx", "co", "byline", "dateline", "in", "tp", "page"]:
                FT.__add_extra_field(doc, atri_doc, extra)

            atri_docs.append(atri_doc)
        return atri_docs

    @staticmethod
    def __add_extra_field(soup, atri_doc, field):
        fval = soup.find(field)
        if fval:
            if isinstance(fval, list):
                for f in fval:
                    atri_doc.add(field, f.text.strip())
            atri_doc.add(field, fval.text.strip())


class FBIS(TREC):

    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        try:
            with p.open(mode='r', encoding='utf-8') as f:  # Reading file
                soup = BeautifulSoup(f, 'lxml')
        except UnicodeDecodeError:
            with p.open(mode='r', encoding='ISO-8859-1') as f:  # Reading file
                soup = BeautifulSoup(f, 'lxml')

        atri_docs = []
        for doc in soup.find_all("doc"):
            doc_no = doc.find("docno").text.strip()
            headline = doc.find("header").text.strip()
            doc_text = re.sub('&blank;', ' ', doc.find("text").text.strip())
            atri_doc = ADoc(doc_no, body=doc_text, headline=headline)
            atri_docs.append(atri_doc)
        return atri_docs


class LATIMES(TREC):
    @staticmethod
    def _resolve(p: Path) -> List[ADoc]:
        with p.open(mode='r') as f:  # Reading file
            soup = BeautifulSoup(f, 'lxml')

        atri_docs = []
        for doc in soup.find_all("doc"):
            doc_no = doc.find("docno").text.strip()
            atri_doc = ADoc(doc_no)
            # optional
            for extra in ["headline", "type", "byline", "date", "section", "graphic", "text"]:
                LATIMES.__add_extra_field(doc, atri_doc, extra)

            atri_docs.append(atri_doc)
        return atri_docs

    @staticmethod
    def __add_extra_field(soup, atri_doc, field):
        fval = soup.find(field)
        if fval:
            if isinstance(fval, list):
                for f in fval:
                    atri_doc.add(field, f.text.strip())
            atri_doc.add(field, fval.text.strip())
