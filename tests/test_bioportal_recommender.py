import pytest
from corporachar.tasks import bioportal_annotator

def test_make_request():
    test_input = """Melanoma is a malignant tumor of melanocytes
                    which are found predominantly in skin but also
                    in the bowel and the eye."""
    r = bioportal_annotator.get_recommendations(test_input)
    assert r == 0