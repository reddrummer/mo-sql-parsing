# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from unittest import TestCase

from mo_parsing.debug import Debugger

from mo_sql_parsing import parse, parse_mysql, normal_op, format


class TestMySql(TestCase):
    def test_issue_22(self):
        sql = 'SELECT "fred"'
        result = parse_mysql(sql)
        expected = {"select": {"value": {"literal": "fred"}}}
        self.assertEqual(result, expected)

    def test_issue_126(self):
        from mo_sql_parsing import parse_mysql as parse

        result = parse(
            """SELECT algid, item_id, avg(score) as avg_score 
            from (
                select 
                    ds, 
                    algid, 
                    uin, 
                    split(item_result, ":")[0] as item_id, 
                    split(item_result, ": ")[1] as score 
                from (
                    select ds, scorealgid_ as algid, uin_ as uin, item_result 
                    from (
                        select * 
                        from t_dwd_tmp_wxpay_discount_model_score_hour 
                        where ds >= 2022080300 and ds <= 2022080323
                    )day_tbl 
                    LATERAL VIEW explode(split(scoreresult_, "\\ | ")) temp AS item_result
                ) t
            ) tbl 
            group by algid, item_id;"""
        )
        expected = {
            "from": {
                "name": "tbl",
                "value": {
                    "from": {
                        "name": "t",
                        "value": {
                            "from": [
                                {
                                    "name": "day_tbl",
                                    "value": {
                                        "from": "t_dwd_tmp_wxpay_discount_model_score_hour",
                                        "select": "*",
                                        "where": {"and": [{"gte": ["ds", 2022080300]}, {"lte": ["ds", 2022080323]}]},
                                    },
                                },
                                {"lateral view": {
                                    "name": {"temp": "item_result"},
                                    "value": {"explode": {"split": ["scoreresult_", {"literal": "\\ | "}]}},
                                }},
                            ],
                            "select": [
                                {"value": "ds"},
                                {"name": "algid", "value": "scorealgid_"},
                                {"name": "uin", "value": "uin_"},
                                {"value": "item_result"},
                            ],
                        },
                    },
                    "select": [
                        {"value": "ds"},
                        {"value": "algid"},
                        {"value": "uin"},
                        {"name": "item_id", "value": {"get": [{"split": ["item_result", {"literal": ":"}]}, 0]}},
                        {"name": "score", "value": {"get": [{"split": ["item_result", {"literal": ": "}]}, 1]}},
                    ],
                },
            },
            "groupby": [{"value": "algid"}, {"value": "item_id"}],
            "select": [{"value": "algid"}, {"value": "item_id"}, {"name": "avg_score", "value": {"avg": "score"}}],
        }
        self.assertEqual(result, expected)

    def test_issue_157_describe1(self):
        sql = """Explain format=traditional select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "traditional"}
        self.assertEqual(result, expected)

    def test_issue_157_describe2(self):
        sql = """desc format=tree select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "tree"}
        self.assertEqual(result, expected)

    def test_issue_157_describe3(self):
        sql = """desc format=json select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "json"}
        self.assertEqual(result, expected)

    def test_merge_into(self):
        sql = """
            MERGE INTO TMP_TABLE1 TMP1
            USING TMP_TABLE1 TMP2
            ON TMP1.col1 =TMP2.col1
            AND TMP1.col2=TMP2.col2
            AND TMP1.col3=TMP2.col3
            AND (TMP2.col4 - 1) = TMP1.col4
            WHEN MATCHED THEN
            UPDATE SET ZTAGG_END = TMP2.ZTAGG"""
        result = parse(sql)
        expected = {
            "merge": {"when": "matched", "then": {"update": {"ZTAGG_END": "TMP2.ZTAGG"}}},
            "target": {"name": "TMP1", "value": "TMP_TABLE1"},
            "source": {"name": "TMP2", "value": "TMP_TABLE1"},
            "on": {"and": [
                {"eq": ["TMP1.col1", "TMP2.col1"]},
                {"eq": ["TMP1.col2", "TMP2.col2"]},
                {"eq": ["TMP1.col3", "TMP2.col3"]},
                {"eq": [{"sub": ["TMP2.col4", 1]}, "TMP1.col4"]},
            ]},
        }
        self.assertEqual(result, expected)

    def test_merge_top(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """
            MERGE  TOP 10 percent
            TargetProducts AS Target
            USING SourceProducts AS Source
            ON Source.ProductID = Target.ProductID
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price);
        """
        result = parse(sql)
        expected = {
            "merge": {
                "then": {
                    "insert": {},
                    "columns": ["ProductID", "ProductName", "Price"],
                    "values": ["Source.ProductID", "Source.ProductName", "Source.Price"],
                },
                "when": "not_matched_by_target",
            },
            "top": {"percent": 10},
            "on": {"eq": ["Source.ProductID", "Target.ProductID"]},
            "source": {"name": "Source", "value": "SourceProducts"},
            "target": {"name": "Target", "value": "TargetProducts"},
        }
        self.assertEqual(result, expected)

    def test_merge1(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """
            MERGE TargetProducts AS Target
            USING SourceProducts AS Source
            ON Source.ProductID = Target.ProductID
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price);
        """
        result = parse(sql)
        expected = {
            "merge": {
                "then": {
                    "insert": {},
                    "columns": ["ProductID", "ProductName", "Price"],
                    "values": ["Source.ProductID", "Source.ProductName", "Source.Price"],
                },
                "when": "not_matched_by_target",
            },
            "on": {"eq": ["Source.ProductID", "Target.ProductID"]},
            "source": {"name": "Source", "value": "SourceProducts"},
            "target": {"name": "Target", "value": "TargetProducts"},
        }
        self.assertEqual(result, expected)

    def test_merge2(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """
            MERGE TargetProducts AS Target
            USING SourceProducts AS Source
            ON Source.ProductID = Target.ProductID
            
            -- For Inserts
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price)
            
            -- For Updates
            WHEN MATCHED THEN UPDATE SET
                Target.ProductName = Source.ProductName,
                Target.Price  = Source.Price;
        """
        result = parse(sql)
        expected = {
            "merge": [
                {
                    "then": {
                        "columns": ["ProductID", "ProductName", "Price"],
                        "insert": {},
                        "values": ["Source.ProductID", "Source.ProductName", "Source.Price"],
                    },
                    "when": "not_matched_by_target",
                },
                {
                    "then": {"update": {"Target.Price": "Source.Price", "Target.ProductName": "Source.ProductName"}},
                    "when": "matched",
                },
            ],
            "on": {"eq": ["Source.ProductID", "Target.ProductID"]},
            "source": {"name": "Source", "value": "SourceProducts"},
            "target": {"name": "Target", "value": "TargetProducts"},
        }
        self.assertEqual(result, expected)

    def test_merge3(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """ 
            MERGE TargetProducts AS Target
            USING SourceProducts AS Source
            ON Source.ProductID = Target.ProductID
                
            -- For Inserts
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price)
                
            -- For Updates
            WHEN MATCHED THEN UPDATE SET
                Target.ProductName = Source.ProductName,
                Target.Price  = Source.Price
                
            -- For Deletes
            WHEN NOT MATCHED BY Source THEN
                DELETE;        
        """
        result = parse(sql)
        expected = {
            "merge": [
                {
                    "then": {
                        "columns": ["ProductID", "ProductName", "Price"],
                        "insert": {},
                        "values": ["Source.ProductID", "Source.ProductName", "Source.Price"],
                    },
                    "when": "not_matched_by_target",
                },
                {
                    "then": {"update": {"Target.Price": "Source.Price", "Target.ProductName": "Source.ProductName"}},
                    "when": "matched",
                },
                {"then": {"delete": {}}, "when": "not_matched_by_source"},
            ],
            "on": {"eq": ["Source.ProductID", "Target.ProductID"]},
            "source": {"name": "Source", "value": "SourceProducts"},
            "target": {"name": "Target", "value": "TargetProducts"},
        }
        self.assertEqual(result, expected)

    def test_issue_180_subsquery(self):
        sql = """SELECT concat( 
            ( SELECT value FROM schema.table a where b = c ), 
            RIGHT( '00' + CAST(MONTH(d) AS VARCHAR(2)), 2 ) 
        ) AS res"""
        result = parse(sql)
        expected = {"select": {
            "name": "res",
            "value": {"concat": [
                {
                    "from": {"name": "a", "value": "schema.table"},
                    "select": {"value": "value"},
                    "where": {"eq": ["b", "c"]},
                },
                {"right": [{"add": [{"literal": "00"}, {"cast": [{"month": "d"}, {"varchar": 2}]}]}, 2]},
            ]},
        }}
        self.assertEqual(result, expected)

    def test_issue_187_using_btree(self):
        sql = """create table tt (n varchar(10), nn varchar(6), key `inx_nn` (`nn`) using btree );"""
        result = parse(sql)
        expected = {"create table": {
            "columns": [{"name": "n", "type": {"varchar": 10}}, {"name": "nn", "type": {"varchar": 6}}],
            "constraint": {"index": {"columns": "nn", "name": "inx_nn", "using": "btree"}},
            "name": "tt",
        }}
        self.assertEqual(result, expected)

    def test_using_btree_in_primary_key_1(self):
        sql = """create table tt (n varchar(10), nn varchar(6), primary key (`nn`) using btree );"""
        result = parse(sql)
        expected = {"create table": {
            "columns": [{"name": "n", "type": {"varchar": 10}}, {"name": "nn", "type": {"varchar": 6}}],
            "constraint": {"primary_key": {"columns": "nn", "using": "btree"}},
            "name": "tt",
        }}
        self.assertEqual(result, expected)

    def test_using_btree_in_primary_key_2(self):
        sql = """create table tt (n varchar(10), nn varchar(6), primary key using btree (`nn`) );"""
        result = parse(sql)
        expected = {"create table": {
            "columns": [{"name": "n", "type": {"varchar": 10}}, {"name": "nn", "type": {"varchar": 6}}],
            "constraint": {"primary_key": {"columns": "nn", "using": "btree"}},
            "name": "tt",
        }}
        self.assertEqual(result, expected)

    def test_issue_186_limit(self):
        sql = """SELECT * from t1 limit 1,10"""
        result = parse(sql)
        expected = {"select": "*", "from": "t1", "offset": 1, "limit": 10}
        self.assertEqual(result, expected)

    def test_issue_185_group_concat(self):
        sql = """SELECT substring_index(group_concat(manager.id order by manager.create DESC separator ','), ',', 1) AS s_id FROM a"""
        result = parse(sql)
        expected = {
            "from": "a",
            "select": {
                "name": "s_id",
                "value": {"substring_index": [
                    {
                        "group_concat": "manager.id",
                        "orderby": {"sort": "desc", "value": "manager.create"},
                        "separator": {"literal": ","},
                    },
                    {"literal": ","},
                    1,
                ]},
            },
        }
        self.assertEqual(result, expected)

    def test_issue_185_on_update(self):
        sql = """create table a (lastcreated datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"""
        result = parse(sql)
        expected = {"create table": {
            "name": "a",
            "columns": {
                "default": "CURRENT_TIMESTAMP",
                "name": "lastcreated",
                "on_update": "CURRENT_TIMESTAMP",
                "type": {"datetime": {}},
            },
        }}
        self.assertEqual(result, expected)

    def test_issue_185_algorithm(self):
        # CREATE [OR REPLACE][ALGORITHM = {MERGE | TEMPTABLE | UNDEFINED}] VIEW
        sql = """CREATE OR REPLACE ALGORITHM = UNDEFINED VIEW view_name AS SELECT *"""
        result = parse(sql)
        expected = {"create view": {
            "algorithm": "undefined",
            "name": "view_name",
            "query": {"select": "*"},
            "replace": True,
        }}

        self.assertEqual(result, expected)

    def test_issue_185_index_length(self):
        sql = """CREATE TABLE IF NOT EXISTS industry_i18n (id bigint(20) NOT NULL AUTO_INCREMENT, KEY locale_id (master_id,locale(255)));"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"auto_increment": True, "name": "id", "nullable": False, "type": {"bigint": 20}},
            "constraint": {"index": {
                "name": "locale_id",
                "columns": ["master_id", {"value": "locale", "length": 255}],
            }},
            "name": "industry_i18n",
            "replace": False,
        }}
        self.assertEqual(result, expected)

    def test_regexp(self):
        sql = """SELECT * FROM dbo.table where col  REGEXP '[^0-9A-Za-z]'"""
        result = parse(sql)
        expected = {"select": "*", "from": "dbo.table", "where": {"regexp": ["col", {"literal": "[^0-9A-Za-z]"}]}}
        self.assertEqual(result, expected)

    def test_not_regexp(self):
        sql = """SELECT * FROM dbo.table where col NOT REGEXP '[^0-9A-Za-z]'"""
        result = parse(sql)
        expected = {"select": "*", "from": "dbo.table", "where": {"not_regexp": ["col", {"literal": "[^0-9A-Za-z]"}]}}
        self.assertEqual(result, expected)

    def test_issue_192_delete1(self):
        sql = "DELETE FROM a LIMIT 1"
        result = parse(sql)
        expected = {"delete": "a", "limit": 1}
        self.assertEqual(result, expected)

    def test_issue_192_delete2(self):
        sql = "DELETE FROM a ORDER BY a.f DESC"
        result = parse(sql)
        expected = {"delete": "a", "orderby": {"sort": "desc", "value": "a.f"}}
        self.assertEqual(result, expected)

    def test_issue_192_delete3(self):
        sql = "DELETE LOW_PRIORITY FROM a"
        result = parse(sql)
        expected = {"delete": "a", "low_priority": True}
        self.assertEqual(result, expected)

    def test_issue_192_delete4(self):
        sql = "DELETE QUICK FROM a"
        result = parse(sql)
        expected = {"delete": "a", "quick": True}
        self.assertEqual(result, expected)

    def test_issue_192_delete5(self):
        sql = "DELETE IGNORE FROM a"
        result = parse(sql)
        expected = {"delete": "a", "ignore": True}
        self.assertEqual(result, expected)

    def test_issue_192_delete6(self):
        sql = "DELETE LOW_PRIORITY FROM a"
        result = parse(sql)
        expected = {"delete": "a", "low_priority": True}
        self.assertEqual(result, expected)

    def test_issue_192_delete7(self):
        sql = "DELETE t1 FROM table1 t1 WHERE t1.a IN (SELECT t2.a FROM table2 t2)"
        result = parse(sql)
        expected = {
            "delete": "t1",
            "from": {"name": "t1", "value": "table1"},
            "where": {"in": ["t1.a", {"from": {"name": "t2", "value": "table2"}, "select": {"value": "t2.a"}}]},
        }
        self.assertEqual(result, expected)

    def test_issue_192_delete8(self):
        sql = "DELETE a1, a2 FROM t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id"
        result = parse(sql)
        expected = {
            "delete": ["a1", "a2"],
            "from": [{"name": "a1", "value": "t1"}, {"inner join": {"name": "a2", "value": "t2"}}],
            "where": {"eq": ["a1.id", "a2.id"]},
        }

        self.assertEqual(result, expected)

    def test_issue_192_delete9(self):
        sql = "DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id"
        result = parse(sql)
        expected = {
            "delete": ["a1", "a2"],
            "from": [{"name": "a1", "value": "t1"}, {"inner join": {"name": "a2", "value": "t2"}}],
            "where": {"eq": ["a1.id", "a2.id"]},
        }
        self.assertEqual(result, expected)

    def test_update_order_by_limit(self):
        """
        refer: https://dev.mysql.com/doc/refman/5.7/en/update.html
        """
        sql = "UPDATE tb1 a SET a1='', a2 = now() WHERE a3='' ORDER BY a4 LIMIT 1, 2"
        result = parse(sql)
        expected = {
            "update": {"name": "a", "value": "tb1"},
            "set": {"a1": {"literal": ""}, "a2": {"now": {}}},
            "where": {"eq": ["a3", {"literal": ""}]},
            "orderby": {"value": "a4"}, "limit": 2, "offset": 1
        }
        self.assertEqual(result, expected)

    def test_update_join(self):
        """
        refer: https://dev.mysql.com/doc/refman/5.7/en/update.html
        """
        sql = """
            UPDATE tb1 a LEFT JOIN tb2 b ON a.a1 = b.b1 LEFT JOIN tb3 c ON a.a2 = c.c2 
            SET a.a3 = b.b3, a.a4 = c.c4 
            WHERE a.a5 = ''
        """
        result = parse(sql)
        expected = {
            "set": {"a.a3": "b.b3", "a.a4": "c.c4"},
            "update": [
                {"name": "a", "value": "tb1"},
                {"left join": {"name": "b", "value": "tb2"}, "on": {"eq": ["a.a1", "b.b1"]}},
                {"left join": {"name": "c", "value": "tb3"}, "on": {"eq": ["a.a2", "c.c2"]}}
            ],
            "where": {"eq": ["a.a5", {"literal": ""}]}}
        self.assertEqual(result, expected)
