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
CRUD of MCols
"""
from typing import Optional, List

from fastapi import APIRouter, HTTPException, UploadFile
from pydantic import BaseModel

from atri.api.strings import get_text
from atri.core import ACol
from atri.manager import fs, FileSystemError

router = APIRouter(
    prefix="/collection",
    tags=["collection"],
    responses={
        404: {"description": "Collection not found"},
        409: {"description": "Collection already exists"},
    }
)


class CollectionUpdateModel(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    search_defaults: Optional[dict] = None
    index_defaults: Optional[dict] = None


class CollectionCreateModel(BaseModel):
    name: str
    description: Optional[str] = None


class DocumentListPageModel(BaseModel):
    documents: List[dict]
    current_page: int
    total_pages: int


@router.get("/{collection_name}")
def get_collection(collection_name: str, page: int = 1):
    """
    Get a collection of Atri given its name. The collection returned could have multiple documents and thus
    could be paginated. The page number is 1-based.
    :param collection_name: The name of the collection to be returned.
    :param page: Page number of the collection to be returned.
    :return:
    """
    try:
        atri_col: ACol = fs.collections[collection_name]
        collection_json = atri_col.json()
        paths, current_page, pages = atri_col.document_paths(page)

        # sanity check: if the page number is out of range, return an error.
        if page > pages:
            raise HTTPException(status_code=404, detail="Page not found")

        files_json = list(map(fs.file_info, paths))
        response = {
            "collection": collection_json,
            "files": files_json,
            "current_page": current_page,
            "pages": pages
        }
        return response
    except (FileSystemError, KeyError) as er:
        raise HTTPException(status_code=404, detail=str(er))


@router.put("/{collection_name}", status_code=201)
def put_collection(collection_name: str, collection_json: CollectionUpdateModel):
    """
    Update a collection given its name.
    :param collection_name: The name of the collection to be created.
    :param collection_json: Body of the request containing the collection to be updated.
    :return:
    """
    try:
        fs.update_collection(
            collection_name, name=collection_json.name, description=collection_json.description,
            index_defaults=collection_json.index_defaults, search_defaults=collection_json.search_defaults
        )
    except (FileSystemError, KeyError) as er:
        raise HTTPException(status_code=409, detail=str(er))

    return {
        "success": get_text("collection_saved").format(collection_name)
    }


@router.delete("/{collection_name}", status_code=204)
def delete_collection(collection_name: str):
    """
    Delete a collection given its name.
    :param collection_name: The name of the collection to be deleted.
    :return:
    """
    try:
        fs.remove(collection_name)
    except FileSystemError as er:
        raise HTTPException(status_code=404, detail=str(er))

    return {
        "success": get_text("collection_deleted").format(collection_name)
    }


@router.post("/", status_code=201)
def post_collection(collection_json: CollectionCreateModel):
    """
    Create a collection given its name.
    :param collection_json: Body of the request containing the collection to be created/updated.
    :return:
    """
    try:
        fs.add(
            collection_json.name, description=collection_json.description
        )
    except (FileSystemError, KeyError) as er:
        raise HTTPException(status_code=409, detail=str(er))

    return {
        "success": get_text("collection_saved").format(collection_json.name)
    }


@router.get("/", status_code=200)
def get_all_collections():
    """
    Get all collections.
    :return: Information about all collections.
    """
    try:
        total_size = 0.0
        collections: List[ACol] = [c for c in fs.get_all()]
        collections_json = []
        for col in collections:
            collections_json.append(col.json())
            total_size += col.size()
        return {
            "collections": collections_json,
            "total_MB": total_size
        }
    except FileSystemError as er:
        raise HTTPException(status_code=404, detail=str(er))


@router.get("/{collection_name}/{file_name}", status_code=200)
def get_file(collection_name: str, file_name: str):
    """
    Get a file given its collection and file name.
    :param collection_name: The name of the collection containing the file.
    :param file_name: The name of the file to be returned.
    :return: File contents.
    """
    try:
        return fs.collection_manager(collection_name).get_doc(file_name).json()
    except FileSystemError as er:
        raise HTTPException(status_code=404, detail=str(er))


@router.delete("/{collection_name}/{file_name}", status_code=200)
def delete_file(collection_name: str, file_name: str):
    """
    Delete a file given its collection and file name.
    :param collection_name: The name of the collection containing the file.
    :param file_name: The name of the file to be deleted.
    """
    try:
        fs.collection_manager(collection_name).delete_doc(file_name)
        return {
            "success": get_text("file_removed").format(file_name)
        }
    except Exception as er:
        raise HTTPException(status_code=404, detail=str(er))


@router.post("/{collection_name}/file/upload", status_code=200)
def upload_files(collection_name: str, files: List[UploadFile]):
    """
    Upload a file to a collection.
    :param collection_name: The name of the collection to be uploaded to.
    :param files: The files to be uploaded.
    """
    try:
        for f in files:
            fs.collection_manager(collection_name).add_doc(f.filename, f.file.read())
    except FileSystemError as er:
        raise HTTPException(status_code=409, detail=str(er))

    return {
        "success": get_text("multiple_file_uploaded").format(len(files))
    }


@router.get("/{collection_name}/file/list", response_model=DocumentListPageModel, status_code=200)
def filelist(collection_name: str, page: int = 1):
    """
    Get a list of files in a collection.
    :param collection_name: The name of the collection.
    :param page: The page number. Default is 1.
    :return: All files in the collection.
    """
    try:
        docs, current_page, total_pages = fs.collection_manager(collection_name).all_document_locations(page)
        doc_infos = list(map(fs.file_info, docs))
        return DocumentListPageModel(
            documents=doc_infos,
            current_page=current_page,
            total_pages=total_pages
        )
    except FileSystemError as er:
        raise HTTPException(status_code=404, detail=str(er))
