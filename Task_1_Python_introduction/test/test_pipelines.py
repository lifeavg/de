from datetime import datetime
from unittest.mock import MagicMock, patch

import pipelines as ppl


class TestJSONPipe:
    @patch.multiple(ppl.JSONPipe, __abstractmethods__=set())
    def test_filter_ok(self):
        pipe = ppl.JSONPipe("string")  # type: ignore # pylint: disable=E0110
        pipe.is_valid = MagicMock(return_value=(True, "ok"))
        assert list(pipe.filter([{"a": "b"}])) == [{"a": "b"}]

    @patch.multiple(ppl.JSONPipe, __abstractmethods__=set())
    def test_filter_filtered(self):
        pipe = ppl.JSONPipe("string")  # type: ignore # pylint: disable=E0110
        pipe.is_valid = MagicMock(return_value=(False, "reason"))
        assert list(pipe.filter([{"a": "b"}])) == []


class TestStudentPipe:
    def test_transform_ok(self):
        pipe = ppl.StudentPipe("string")  # type: ignore # pylint: disable=E0110
        pipe.is_valid = MagicMock(return_value=(True, "ok"))
        assert list(pipe.transform([{"birthday": "2004-01-07T00:00:00.000000"}])) == [
            {"birthday": datetime(2004, 1, 7).date()}
        ]

    @patch.multiple(ppl.JSONPipe, __abstractmethods__=set())
    def test_transform_birthday_error(self):
        pipe = ppl.JSONPipe("string")  # type: ignore # pylint: disable=E0110
        pipe.is_valid = MagicMock(return_value=(True, "ok"))
        assert pipe.transform([{"birthday": "20dd04-01-07T00:00:00.000000"}]) is None
