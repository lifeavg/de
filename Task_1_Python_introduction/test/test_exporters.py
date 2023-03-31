from exporters import to_json, to_xml

data = [{"a": "aa", "b": 1}]


def test_to_json():
    assert to_json(data) == '[{"a": "aa", "b": 1}]'


def test_to_xml():
    assert to_xml(data) == '<?xml version="1.0" encoding="UTF-8" ?><root><item><a>aa</a><b>1</b></item></root>'
