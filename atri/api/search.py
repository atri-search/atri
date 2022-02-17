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
import json
import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, UploadFile, Form
from pydantic import BaseModel

from atri.manager import fs, FileSystemError, QueryManager

router = APIRouter(
    prefix="/search",
    tags=["search"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    }
)


class QueryModel(BaseModel):
    query: str
    advanced_options: Optional[dict] = {}


class FileQueryModel(BaseModel):
    multiquery: bool = False
    options_payload: Optional[Dict[str, Any]] = {}


class HitsModel(BaseModel):
    name: str
    score: float
    position: int
    summary: Optional[str] = None


class QueryResponseModel(BaseModel):
    hits: List[HitsModel]
    time: float


class MultiQueryResponseModel(BaseModel):
    reports: Dict[str, dict]


@router.post("/{collection_name}", response_model=QueryResponseModel, status_code=200)
def search(collection_name: str, query: QueryModel):
    """
    Search documents in a collection.
    :param collection_name: Name of the collection to search with.
    :param query: Query object.
    :return: QueryResponse object.
    """
    try:
        query_keywords = query.query
        advanced_query_parameters = query.advanced_options

        start_time = datetime.datetime.now()

        # search
        hits = fs.search_manager(collection_name).search(query_keywords, **advanced_query_parameters)

        end_time = datetime.datetime.now()
        time = (end_time - start_time).total_seconds() * 1000

        # extracting List[HitsModel] from List[dict]
        hits_model = get_hits_model(hits)
        return QueryResponseModel(hits=hits_model, time=time)

    except FileSystemError as e:
        raise HTTPException(status_code=406, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_hits_model(hits: Dict[str, Any]) -> List[HitsModel]:
    hits_model = []
    for name, info in hits.items():
        hits_model.append(
            HitsModel(
                name=name,
                score=info["score"],
                position=info["position"],
                summary=info.get("summary", None)
            )
        )
    return hits_model


@router.post("/{collection_name}/file", status_code=200)
async def file_search(collection_name: str, files: List[UploadFile],
                      multiquery: bool = Form(...),
                      options_payload: str = Form(...)):
    """
    Search documents in a collection.
    :param collection_name: Name of the collection to search with.
    :param files: List of files to search.
    :param multiquery: True if the query is a multiquery.
    :param options_payload: JSON string with advanced query options.
    :return: QueryResponse object.
    """
    try:
        query_metadata = FileQueryModel(multiquery=multiquery, options_payload=json.loads(options_payload))
        if query_metadata.multiquery:
            return await __multi_query(collection_name, query_metadata.options_payload, files)
        else:
            return await __compose_query(collection_name, query_metadata.options_payload, files)
    except FileSystemError as e:
        raise HTTPException(status_code=406, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def __multi_query(collection_name: str, query_info: Dict[str, Any],
                        files_search: List[UploadFile]) -> MultiQueryResponseModel:
    reports = {}

    for file in files_search:
        query_generator = fs.query_manager(file.filename, await file.read()).parse(True)
        reports[file.filename] = fs.search_manager(collection_name) \
            .multiquery_search(query_generator, **query_info)

    return MultiQueryResponseModel(reports=reports)


async def __compose_query(collection_name: str, query_info: Dict[str, Any],
                          files_search: List[UploadFile]) -> QueryResponseModel:
    keywords = ""

    for file in files_search:
        adocs = fs.query_manager(file.filename, await file.read()).parse(False)
        keywords += QueryManager.keywords_query(adocs, query_info.get("fields", None))

    start_time = datetime.datetime.now()

    hits = fs.search_manager(collection_name).search(keywords, **query_info)

    end_time = datetime.datetime.now()
    time = (end_time - start_time).total_seconds() * 1000

    # extracting List[HitsModel] from List[dict]
    hits_model = get_hits_model(hits)
    return QueryResponseModel(hits=hits_model, time=time)
