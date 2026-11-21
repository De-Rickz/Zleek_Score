"""
Tests for parse_token_ids logging (TASKS T5): malformed or wrong-length
clobTokenIds must be logged with the market id and raw value; a good pair is
not logged. Behaviour is unchanged: bad input leaves token ids unset.

    cd apps/worker && .venv/bin/python tests/test_gamma_parse.py
"""
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import gamma_client as g


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def parse(raw_ids):
    cap = Capture()
    g.logger.addHandler(cap)
    try:
        rows = g.parse_markets([{"id": "m1", "question": "Q?", "conditionId": "0xabc",
                                 "clobTokenIds": raw_ids}])
    finally:
        g.logger.removeHandler(cap)
    assert len(rows) == 1
    return rows[0], cap.messages


def test_good_pair_no_log():
    row, msgs = parse('["111", "222"]')
    assert row["token_yes_id"] == "111" and row["token_no_id"] == "222"
    assert msgs == []


def test_malformed_json_logged():
    row, msgs = parse('["111", "22')
    assert row["token_yes_id"] is None and row["token_no_id"] is None
    assert len(msgs) == 1 and "m1" in msgs[0] and '["111", "22' in msgs[0]


def test_one_id_logged():
    row, msgs = parse('["111"]')
    assert row["token_yes_id"] is None
    assert len(msgs) == 1 and "m1" in msgs[0] and '["111"]' in msgs[0]


def test_three_ids_logged():
    row, msgs = parse('["1", "2", "3"]')
    assert row["token_yes_id"] is None and row["token_no_id"] is None
    assert len(msgs) == 1 and "m1" in msgs[0] and '["1", "2", "3"]' in msgs[0]


def test_missing_or_empty_not_logged():
    assert parse("")[1] == []
    assert parse(None)[1] == []


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
