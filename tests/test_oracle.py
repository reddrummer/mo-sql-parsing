# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import re
from unittest import TestCase

from mo_files import File
from mo_logs import strings
from mo_parsing.debug import Debugger
from mo_testing.fuzzytestcase import add_error_reporting

from mo_sql_parsing import parse


@add_error_reporting
class TestOracle(TestCase):
    def test_issue_90_tablesample1(self):
        sql = "SELECT * FROM foo SAMPLE bernoulli (1) WHERE a < 42"
        result = parse(sql)
        expected = {
            "from": {"tablesample": {"method": "bernoulli", "percent": 1}, "value": "foo"},
            "select": {"all_columns": {}},
            "where": {"lt": ["a", 42]},
        }
        self.assertEqual(result, expected)

    def test_issue_90_tablesample2(self):
        sql = "SELECT * FROM foo SAMPLE(1) WHERE a < 42"
        result = parse(sql)
        expected = {
            "from": {"tablesample": {"percent": 1}, "value": "foo"},
            "select": {"all_columns": {}},
            "where": {"lt": ["a", 42]},
        }
        self.assertEqual(result, expected)

    def test_issue_157_describe(self):
        sql = """describe into s.t@database for select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": {"all_columns": {}}}, "into": "s.t@database"}
        self.assertEqual(result, expected)

    def test_issue_157_describe2(self):
        sql = """explain plan into s.t@database for select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": {"all_columns": {}}}, "into": "s.t@database"}
        self.assertEqual(result, expected)

    def test_natural_join(self):
        sql = """select * from A natural join b"""
        result = parse(sql)
        expected = {"select": {"all_columns": {}}, "from": ["A", {"natural join": "b"}]}
        self.assertEqual(result, expected)

    def test_validate_conversion_parsing(self):
        query = """SELECT VALIDATE_CONVERSION(a AS DECIMAL(10, 3)) FROM b.c"""
        result = parse(query)
        expected = {
            "select": {"value": {"validate_conversion": ["a", {"decimal": [10, 3]}]}},
            "from": "b.c",
        }
        self.assertEqual(result, expected)

    def test_issue_218_udf(self):

        def lines():
            delimiter = ";"
            splitter = re.compile(r";\s*\n")
            content = File("tests/oracle/issue_218.sql").read()
            while content:
                sql, content = splitter.split(content, 1)
                result = parse(sql+delimiter)
                if result and "delimiter" in result:
                    with Debugger():
                        parse(sql + delimiter)
                    delimiter = result["delimiter"].strip()
                    splitter = re.compile(re.escape(delimiter) + r"\s*\n")
                yield result

        from tests.oracle.issue_218 import expectations
        for i, (result, expected) in enumerate(zip(lines(), expectations)):
            self.assertEqual(result, expected)

    def test_issue_218_comment(self):
        sql = """/*!50610 SET @@default_storage_engine = 'InnoDB'*/;"""
        result = parse(sql)
        self.assertIsNone(result)
