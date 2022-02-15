import shutil
import pytest
from pathlib import Path

from atri.core.index.index import AIndexer
from atri.core.primitives import ADoc, ACol
from atri.core.ranking.search import ASearcher
from atri.error import AtriError

TEMPORARY_FOLDER = "./_temporary"


def create_temp_path():
    p = Path(TEMPORARY_FOLDER)
    if p.exists():
        p.unlink()
    return p


def remove_temp_path():
    p = Path(TEMPORARY_FOLDER)
    if p.exists():
        shutil.rmtree(p)


def test_atri_documents():
    fields = {
        "title": b"Title of document",
        "body": b"Body of document",
        "path": b"Path of document",
        "created_at": "Today"
    }

    atri_doc = ADoc(
        "Test Document",
        **fields
    )

    assert atri_doc.name == "Test Document"

    atri_doc.name = "Changed name"

    assert atri_doc.name == "Changed name"

    assert atri_doc.fields == fields

    atri_doc.fields['new_field'] = b"New bytes content"

    assert 'new_field' in atri_doc.fields

    atri_doc.add('another_field', "Another value")

    assert 'another_field' in atri_doc.fields

    json = atri_doc.json()

    assert 'name' in json and 'fields' in json and len(json['fields']) == 6


def test_collections():
    atri_docs = [
        ADoc("d1", body="Romário Brasil"),
        ADoc("d2", body="Maradona Argentina"),
        ADoc("d3", body="Ronaldo Brasil")
    ]

    p = create_temp_path()

    with pytest.raises(AtriError):
        invalid_col = ACol("MyCorpus", "Test collection", p, loading=True,
                           replace=True, atri_docs=atri_docs)

    with pytest.raises(AtriError):
        invalid_col = ACol("MyCorpus", "Test collection", p, loading=False,
                           replace=True, docs=atri_docs)

    remove_temp_path()

    try:
        atri_col = ACol("MyCorpus", "Test collection", p, loading=False, replace=True, atri_docs=atri_docs)

        assert 'd1' in atri_col
        assert atri_col['d1'].name == "d1"
        assert {atri_doc.name for atri_doc in atri_col} == {'d1', 'd2', 'd3'}
        assert atri_col.name == "MyCorpus"

        atri_col.name = "Changed Name"
        assert atri_col.name == "Changed Name"

        assert atri_col.description == "Test collection"
        atri_col.description = "B"
        assert atri_col.description == "B"

        assert atri_col.path == p

        assert atri_col.docs == {'d1', 'd2', 'd3'}
        atri_col.delete_document('d1')
        assert atri_col.docs == {'d2', 'd3'}

        atri_col.add(ADoc("d4", body="Lewandowski Polônia"))
        atri_col.commit()

        del atri_col

        atri_col = ACol.load(p, lazy=False)
        with pytest.raises(AtriError):
            atri_col.add(ADoc("d4", body="Lewandowski Polônia"))

        assert atri_col.docs == {'d2', 'd3', 'd4'}

        test_file = Path("d5.txt")
        with test_file.open(mode="w") as f:
            f.write("Arquivo de teste para processamento")
        atri_col.add_document_from_file(test_file)
        assert atri_col.docs == {'d2', 'd3', 'd4', 'd5'}

        test_file.unlink()
        shutil.rmtree(test_file, ignore_errors=True)

    finally:
        remove_temp_path()


def test_indexing():
    atri_docs = [
        ADoc("d1", body="Romário Brasil"),
        ADoc("d2", body="Maradona Argentina"),
        ADoc("d3", body="Ronaldo Brasil")
    ]

    p = create_temp_path()

    try:
        atri_col = ACol("MyCorpus", "Test collection", p, loading=False, replace=True, atri_docs=atri_docs)
        atri_index = AIndexer(atri_col)
        atri_index.index(clean=True)

        searcher = atri_index.searcher()
        assert searcher.doc_count() == 3
        searcher.close()

        schema = atri_index.get_schema()
        assert {fn for fn, _ in schema.items()} == {'name', 'body', 'path'}

        atri_index.reset()

        atri_index.index(clean=False)

        searcher = atri_index.searcher()
        assert searcher.doc_count() == 3
        searcher.close()

        atri_col.add(ADoc("d4", body="Ronaldo Brasil"))

        atri_index.index(clean=False)

        searcher = atri_index.searcher()
        assert searcher.doc_count() == 4
        searcher.close()

    finally:
        remove_temp_path()


def test_search():
    atri_docs = [
        ADoc("d1", body="Romário Brasil"),
        ADoc("d2", body="Maradona Argentina"),
        ADoc("d3", body="Ronaldo Brasil")
    ]

    p = create_temp_path()

    try:
        atri_col = ACol("MyCorpus", "Test collection", p, loading=False, replace=True, atri_docs=atri_docs)
        atri_index = AIndexer(atri_col)
        atri_index.index(clean=True)

        atri_searcher = ASearcher(atri_index)

        assert atri_searcher.fieldname == 'body'

        results = atri_searcher.search("Brasil AND Ronaldo", similarity="bm25",
                                       b=0.7, k1=1.4)

        assert results[0]['name'] == 'd3'

    finally:
        remove_temp_path()
