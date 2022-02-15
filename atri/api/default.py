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


from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, UploadFile, Depends, File
from pydantic import BaseModel

from atri.api.strings import get_text
from atri.manager import fs, FileSystemError, QueryManager

router = APIRouter(
    prefix="/default",
    tags=["default"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    }
)


class IndexDefaultsModel(BaseModel):
    """
    Defaults for indexing.
    """
    stored: Optional[bool]
    vector: Optional[bool]
    clean: Optional[bool]


@router.get("/index/{collection_name}", response_model=IndexDefaultsModel)
def get_index_defaults(collection_name: str):
    """
    Get defaults for indexing.
    """
    try:
        defaults = fs.collection_manager(collection_name).index_defaults()
        return IndexDefaultsModel(**defaults)
    except FileSystemError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.post("/index/{collection_name}", status_code=201)
def set_index_defaults(collection_name: str, defaults: IndexDefaultsModel):
    """
    Set defaults for indexing.
    """
    try:
        # index defaults ignoring None values
        index_defaults = {
            k: v for k, v in zip(["stored", "vector", "clean"], [defaults.stored, defaults.vector, defaults.clean])
            if v is not None
        }
        fs.update_collection(collection_name, index_defaults=index_defaults)

    except FileSystemError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"success": get_text("collection_saved").format(collection_name)}


@router.get("/similarity/{collection_name}")
def get_search_defaults(collection_name: str):
    """
    Get the similarity of the default files in a collection.
    """
    try:
        return fs.collection_manager(collection_name).search_defaults()
    except FileSystemError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/similarity/{collection_name}", status_code=201)
def set_search_defaults(collection_name: str, defaults: Dict[str, Any]):
    """
    Set defaults for indexing.
    """
    try:
        # index defaults ignoring None values
        fs.update_collection(collection_name, **defaults)

    except FileSystemError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"success": get_text("collection_saved").format(collection_name)}
