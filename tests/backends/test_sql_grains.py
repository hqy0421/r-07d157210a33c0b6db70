# -*- coding: utf-8 -*-
"""SQL-backend tests for fact-grain aware aggregation.

These mirror the rows-backend scenarios against a real SQLite database,
exercising the window-function implementation, conflict probing, hidden
grain columns and the materialized row-wise fallback.
"""

import unittest
import warnings

from bubbles import (
    FieldList, OperationContext, attach_measure_grains,
    GrainConflictError,
)
from bubbles.backends.sql.objects import SQLDataStore
import bubbles.backends.sql.ops
import bubbles.ops.rows

warnings.filterwarnings("ignore")


ORDER_FIELDS = FieldList(
    ("order_id", "integer"),
    ("region", "string"),
    ("order_amt", "number"),
)

LINE_FIELDS = FieldList(
    ("line_id", "integer"),
    ("order_id", "integer"),
    ("line_amt", "number"),
)

ORDERS = [
    (1, "east", 100.0),
    (2, "east", 50.0),
    (3, "west", 20.0),
]

LINES = [
    (10, 1, 30.0),
    (11, 1, 70.0),
    (20, 2, 50.0),
    (30, 3, 20.0),
]


def num(value):
    return float(value) if hasattr(value, "as_tuple") else value


class SQLGrainAggregateTestCase(unittest.TestCase):
    def setUp(self):
        self.context = OperationContext()
        self.context.add_operations_from(bubbles.backends.sql.ops)
        self.context.add_operations_from(bubbles.ops.rows)

        self.store = SQLDataStore("sqlite:///")
        self.orders = self.store.create(
            "orders", ORDER_FIELDS, replace=True)
        self.orders.append_from_iterable(ORDERS)
        self.orders.flush()

        self.lines = self.store.create(
            "lines", LINE_FIELDS, replace=True)
        self.lines.append_from_iterable(LINES)
        self.lines.flush()

        attach_measure_grains(self.orders,
                              {"order_amt": "order_id"})
        attach_measure_grains(self.lines,
                              {"line_amt": "line_id"})

    def _joined(self):
        return self.context.op.join_details(
            self.orders, self.lines, "order_id", "order_id")

    def test_join_fanout_without_amount_multiplication(self):
        joined = self._joined()
        self.assertEqual(
            joined.measure_grains.grain_for("order_amt"), ("order_id",))
        self.assertEqual(
            joined.measure_grains.grain_for("line_amt"), ("line_id",))
        # Four physical rows prove the fan-out really happened.
        self.assertEqual(len(list(joined.rows())), 4)

        result = self.context.op.aggregate(
            joined, "region",
            measures=[("order_amt", "sum"), ("line_amt", "sum")],
            grain_counts=["order_id", "line_id"])

        rows = {r[0]: r for r in result.rows()}
        east = rows["east"]
        self.assertEqual(num(east[1]), 150.0)   # orders 100 + 50
        self.assertEqual(num(east[2]), 150.0)   # lines 30 + 70 + 50
        self.assertEqual(east[3], 2)            # order identities
        self.assertEqual(east[4], 3)            # line identities
        self.assertEqual(east[5], 3)            # physical rows

    def test_equal_amounts_keep_distinct_orders(self):
        t = self.store.create(
            "equal", FieldList(("oid", "integer"),
                               ("g", "string"),
                               ("amount", "number")),
            replace=True)
        t.append_from_iterable([(1, "a", 10.0), (2, "a", 10.0)])
        t.flush()
        attach_measure_grains(t, {"amount": "oid"})

        result = self.context.op.aggregate(
            t, "g", measures=[("amount", "sum")],
            grain_counts=["oid"])
        row = list(result.rows())[0]
        self.assertEqual(num(row[1]), 20.0)
        self.assertEqual(row[2], 2)

    def test_conflict_probe_raises(self):
        bad = self.store.create(
            "bad", FieldList(("oid", "integer"),
                             ("g", "string"),
                             ("amount", "number")),
            replace=True)
        bad.append_from_iterable([(1, "a", 100.0), (1, "a", 120.0)])
        bad.flush()
        attach_measure_grains(bad, {"amount": "oid"})

        with self.assertRaises(GrainConflictError) as cm:
            list(self.context.op.aggregate(
                bad, "g", measures=[("amount", "sum")]).rows())
        self.assertEqual(cm.exception.measure, "amount")
        self.assertEqual(cm.exception.identity, (1,))

    def test_null_semantics(self):
        t = self.store.create(
            "nulls", FieldList(("oid", "integer"),
                               ("g", "string"),
                               ("amount", "number")),
            replace=True)
        t.append_from_iterable([
            (1, "a", None), (1, "a", 40.0),
            (2, "b", None), (2, "b", None),
        ])
        t.flush()
        attach_measure_grains(t, {"amount": "oid"})

        result = self.context.op.aggregate(
            t, "g",
            measures=[("amount", "sum"), ("amount", "min"),
                      ("amount", "max"), ("amount", "average")],
            include_count=False)
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(num(rows["a"][1]), 40.0)
        self.assertIsNone(rows["b"][1])
        self.assertIsNone(rows["b"][2])
        self.assertIsNone(rows["b"][3])
        self.assertIsNone(rows["b"][4])

    def test_hidden_grain_column_keeps_identity(self):
        joined = self._joined()
        filtered = self.context.op.field_filter(
            joined,
            keep=["region", "order_amt", "line_amt", "line_id"])

        self.assertEqual(
            filtered.fields.visible_names(),
            ["region", "order_amt", "line_id", "line_amt"])
        self.assertIn("order_id", filtered.fields.names())
        self.assertTrue(filtered.fields.field("order_id").hidden)

        result = self.context.op.aggregate(
            filtered, "region", measures=[("order_amt", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(num(rows["east"][1]), 150.0)

    def test_rename_propagates(self):
        joined = self._joined()
        renamed = self.context.op.field_filter(
            joined,
            rename={"order_id": "order_no", "order_amt": "total"})
        self.assertEqual(
            renamed.measure_grains.grain_for("total"), ("order_no",))
        result = self.context.op.aggregate(
            renamed, "region", measures=[("total", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(num(rows["east"][1]), 150.0)

    def test_row_filter_then_aggregate(self):
        joined = self._joined()
        survivors = self.context.op.filter_by_value(
            joined, "line_id", 11)
        result = self.context.op.aggregate(
            survivors, "region", measures=[("order_amt", "sum")],
            grain_counts=["order_id"])
        row = list(result.rows())[0]
        self.assertEqual(num(row[1]), 100.0)
        self.assertEqual(row[2], 1)

    def test_keyless_aggregation(self):
        joined = self._joined()
        result = self.context.op.aggregate(
            joined, None, measures=[("order_amt", "sum")],
            grain_counts=["order_id"])
        self.assertEqual(
            result.fields.names(),
            ["order_amt_sum", "order_id_count", "record_count"])
        row = list(result.rows())[0]
        self.assertEqual(num(row[0]), 170.0)
        self.assertEqual(row[1], 3)

    def test_compound_grain_key(self):
        t = self.store.create(
            "compound",
            FieldList(("a", "integer"), ("b", "integer"),
                      ("g", "string"), ("amount", "number")),
            replace=True)
        t.append_from_iterable([
            (1, 9, "x", 100.0), (1, 9, "x", 100.0),
            (1, 8, "x", 50.0),
        ])
        t.flush()
        attach_measure_grains(t, {"amount": ["a", "b"]})

        result = self.context.op.aggregate(
            t, "g",
            measures=[("amount", "sum"), ("amount", "average"),
                      ("amount", "count")],
            grain_counts=[("ab_count", ["a", "b"])])
        row = list(result.rows())[0]
        self.assertEqual(num(row[1]), 150.0)
        self.assertEqual(num(row[2]), 75.0)
        self.assertEqual(row[3], 2)
        self.assertEqual(row[4], 2)

    def test_legacy_sql_aggregation_unchanged(self):
        # No grains attached to this fresh object.
        plain = self.store.create(
            "plain", LINE_FIELDS, replace=True)
        plain.append_from_iterable(LINES)
        plain.flush()

        result = self.context.op.aggregate(
            plain, "order_id", measures=[("line_amt", "sum")])
        self.assertEqual(
            result.fields.names(),
            ["order_id", "line_amt_sum", "record_count"])

    def test_materialized_result_is_natively_consumable(self):
        joined = self._joined()
        result = self.context.op.aggregate(
            joined, "region", measures=[("order_amt", "sum")])
        # The grain-aware SQL path eagerly materializes into rows.
        self.assertIn("rows", result.representations())
        # Re-reading a retained object must work more than once.
        first = list(result.rows())
        second = list(result.rows())
        self.assertEqual(len(first), len(second))

    def test_fallback_when_window_functions_unavailable(self):
        joined = self._joined()
        original_execute = self.store.execute

        def failing(statement, *args, **kwargs):
            if "row_number" in str(statement).lower():
                raise RuntimeError("no window support")
            return original_execute(statement, *args, **kwargs)

        self.store.execute = failing
        try:
            result = self.context.op.aggregate(
                joined, "region",
                measures=[("order_amt", "sum"),
                          ("line_amt", "sum")],
                grain_counts=["order_id", "line_id"])
        finally:
            self.store.execute = original_execute

        # Retry landed on the pure-Python implementation, which still
        # returns a rows object with the correct grain semantics.
        self.assertEqual(result.representations(), ["rows", "records"])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(num(rows["east"][1]), 150.0)
        self.assertEqual(num(rows["east"][2]), 150.0)
        self.assertEqual(rows["east"][3], 2)
        self.assertEqual(rows["east"][4], 3)


if __name__ == "__main__":
    unittest.main()
