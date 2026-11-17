# -*- coding: utf-8 -*-
"""Tests for fact-grain aware aggregation.

Covers the order/line fan-out scenario on both the pure-Python rows
backend and the SQL backend, plus every semantic rule:

* order-level measures contribute once per surviving order identity
* detail-level measures contribute once per detail key
* equal amounts never merge distinct facts (no de-duplication by value)
* conflicting values for one identity raise GrainConflictError
* sum ignores NULL and an all-NULL grain yields NULL
* grain counts count identities, not rows or values
* hidden grain columns keep their identity through field filtering
* renames propagate into the grain declarations
* objects without grain declarations keep the legacy semantics
"""

import unittest

from bubbles import (
    Field, FieldList, RowListDataObject, OperationContext,
    MeasureGrains, GrainMeasure, GrainConflictError,
    attach_measure_grains, prepare_measure_grains,
    merge_measure_grains, FieldFilter,
)
from bubbles.errors import ArgumentError, NoSuchFieldError
import bubbles.ops.rows


# Joined order/line rows. Order 1 fans out to two lines, order 2 to one.
# order_amt is an order-level measure (grain: order_id), line_amt is a
# line-level measure (grain: line_id).
JOINED_FIELDS = FieldList(
    ("order_id", "integer"),
    ("line_id", "integer"),
    ("region", "string"),
    ("order_amt", "number"),
    ("line_amt", "number"),
)

JOINED_ROWS = [
    [1, 10, "east", 100.0, 30.0],
    [1, 11, "east", 100.0, 70.0],
    [2, 20, "east",  50.0, 50.0],
    [3, 30, "west",  20.0, 20.0],
]

GRAINS = {"order_amt": "order_id", "line_amt": "line_id"}


def make_object(rows=None, fields=None, grains=GRAINS):
    obj = RowListDataObject(rows or [list(r) for r in JOINED_ROWS],
                            fields or JOINED_FIELDS)
    if grains is not None:
        attach_measure_grains(obj, grains)
    return obj


class RowsGrainAggregateTestCase(unittest.TestCase):
    def setUp(self):
        self.context = OperationContext()
        self.context.add_operations_from(bubbles.ops.rows)

    def test_order_measure_not_multiplied(self):
        obj = make_object()
        result = self.context.op.aggregate(
            obj, "region",
            measures=[("order_amt", "sum"), ("line_amt", "sum")])

        rows = {r[0]: r for r in result.rows()}
        # 100 + 50, not 100 + 100 + 50
        self.assertEqual(rows["east"][1], 150.0)
        # Detail measure still follows the line grain: 30 + 70 + 50
        self.assertEqual(rows["east"][2], 150.0)
        self.assertEqual(rows["west"][1], 20.0)
        self.assertEqual(rows["west"][2], 20.0)

    def test_grain_counts_count_identities(self):
        obj = make_object()
        result = self.context.op.aggregate(
            obj, "region", measures=[("order_amt", "sum")],
            grain_counts=["order_id", "line_id"])

        self.assertEqual(
            result.fields.names(),
            ["region", "order_amt_sum", "order_id_count",
             "line_id_count", "record_count"])

        rows = {r[0]: r for r in result.rows()}
        # Two distinct orders, three distinct lines, three physical rows
        self.assertEqual(rows["east"][2], 2)
        self.assertEqual(rows["east"][3], 3)
        self.assertEqual(rows["east"][4], 3)
        self.assertEqual(rows["west"][2], 1)
        self.assertEqual(rows["west"][3], 1)

    def test_equal_amounts_do_not_merge_facts(self):
        # Two different orders with the exact same amount must remain two
        # facts; de-duplicating by value would wrongly sum to 10.
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        obj = RowListDataObject(
            [[1, "a", 10.0], [2, "a", 10.0]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        result = self.context.op.aggregate(
            obj, "g", measures=[("amount", "sum")],
            grain_counts=["oid"])
        row = list(result.rows())[0]
        self.assertEqual(row[1], 20.0)
        self.assertEqual(row[2], 2)

    def test_conflict_raised_for_different_values(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        obj = RowListDataObject(
            [[1, "a", 100.0], [1, "a", 120.0]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        with self.assertRaises(GrainConflictError) as cm:
            list(self.context.op.aggregate(
                obj, "g", measures=[("amount", "sum")]).rows())
        self.assertEqual(cm.exception.measure, "amount")
        self.assertEqual(cm.exception.identity, (1,))
        self.assertEqual(cm.exception.values, [100.0, 120.0])

    def test_null_then_value_fills_identity(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        obj = RowListDataObject(
            [[1, "a", None], [1, "a", 40.0]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        result = self.context.op.aggregate(
            obj, "g", measures=[("amount", "sum")], include_count=False)
        self.assertEqual(list(result.rows()), [["a", 40.0]])

    def test_all_null_grain_yields_none(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        obj = RowListDataObject(
            [[2, "b", None], [2, "b", None]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        result = self.context.op.aggregate(
            obj, "g", measures=[("amount", "sum"),
                                ("amount", "min"),
                                ("amount", "max"),
                                ("amount", "average")],
            include_count=False)
        self.assertEqual(
            list(result.rows()), [["b", None, None, None, None]])

    def test_sum_ignores_null_mid_stream(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        obj = RowListDataObject(
            [[1, "a", 10.0], [2, "a", None], [3, "a", 5.0]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        result = self.context.op.aggregate(
            obj, "g", measures=[("amount", "sum")], include_count=False)
        self.assertEqual(list(result.rows()), [["a", 15.0]])

    def test_min_max_average_over_identities(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        # oid 1 repeats (fan-out) with the same value and must count once
        obj = RowListDataObject(
            [[1, "a", 10.0], [1, "a", 10.0], [2, "a", 30.0]], fields)
        attach_measure_grains(obj, {"amount": "oid"})

        result = self.context.op.aggregate(
            obj, "g",
            measures=[("amount", "min"), ("amount", "max"),
                      ("amount", "average")],
            include_count=False)
        self.assertEqual(
            list(result.rows()), [["a", 10.0, 30.0, 20.0]])

    def test_legacy_semantics_without_grains(self):
        obj = make_object(grains=None)
        result = self.context.op.aggregate(
            obj, "region", measures=[("order_amt", "sum")])

        rows = {r[0]: r for r in result.rows()}
        # Plain row-wise sum: order 1 counted twice
        self.assertEqual(rows["east"][1], 250.0)
        self.assertEqual(result.fields.names(),
                         ["region", "order_amt_sum", "record_count"])

    def test_undeclared_measure_stays_row_wise(self):
        # line_amt has no grain here, so it keeps the fan-out row count
        obj = make_object(grains={"order_amt": "order_id"})
        result = self.context.op.aggregate(
            obj, "region",
            measures=[("order_amt", "sum"), ("line_amt", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(rows["east"][1], 150.0)   # grain aware
        self.assertEqual(rows["east"][2], 150.0)   # happens to match here

    def test_keyless_aggregation(self):
        obj = make_object()
        result = self.context.op.aggregate(
            obj, None, measures=[("order_amt", "sum")],
            grain_counts=["order_id", "line_id"])
        self.assertEqual(
            result.fields.names(),
            ["order_amt_sum", "order_id_count", "line_id_count",
             "record_count"])
        row = list(result.rows())[0]
        self.assertEqual(row[0], 170.0)   # 100 + 50 + 20
        self.assertEqual(row[1], 3)
        self.assertEqual(row[2], 4)
        self.assertEqual(row[3], 4)

    def test_compound_grain_key_and_all_aggregates(self):
        fields = FieldList(
            ("a", "integer"), ("b", "integer"),
            ("g", "string"), ("amount", "number"))
        # (1,9) fans out to two equal rows; (1,8) is a second identity.
        obj = RowListDataObject(
            [[1, 9, "x", 100.0], [1, 9, "x", 100.0],
             [1, 8, "x", 50.0]], fields)
        attach_measure_grains(obj, {"amount": ["a", "b"]})

        result = self.context.op.aggregate(
            obj, "g",
            measures=[("amount", "sum"), ("amount", "min"),
                      ("amount", "max"), ("amount", "average"),
                      ("amount", "count")],
            grain_counts=[("ab_count", ["a", "b"])])
        row = list(result.rows())[0]
        self.assertEqual(row[1], 150.0)
        self.assertEqual(row[2], 50.0)
        self.assertEqual(row[3], 100.0)
        self.assertEqual(row[4], 75.0)
        self.assertEqual(row[5], 2)       # two identities
        self.assertEqual(row[6], 2)       # grain count
        self.assertEqual(row[7], 3)       # physical rows

    def test_grain_count_includes_all_null_identity(self):
        fields = FieldList(("oid", "integer"), ("g", "string"),
                           ("amount", "number"))
        # oid 2 exists as a fact but its amount is always NULL.
        obj = RowListDataObject(
            [[1, "a", 10.0], [2, "a", None]], fields)
        attach_measure_grains(obj, {"amount": "oid"})
        result = self.context.op.aggregate(
            obj, "g", measures=[("amount", "count")],
            grain_counts=["oid"], include_count=False)
        row = list(result.rows())[0]
        self.assertEqual(row[1], 2)
        self.assertEqual(row[2], 2)

    def test_row_filter_then_aggregate_uses_survivors(self):
        obj = make_object()
        filtered = self.context.op.filter_by_value(
            obj, "line_id", 11)
        result = self.context.op.aggregate(
            filtered, "region", measures=[("order_amt", "sum")],
            grain_counts=["order_id"])
        row = list(result.rows())[0]
        # Only order 1 survives; it contributes once
        self.assertEqual(row[1], 100.0)
        self.assertEqual(row[2], 1)

    def test_grain_propagates_through_unary_filters(self):
        obj = make_object()
        filtered = self.context.op.filter_not_empty(obj, "line_amt")
        self.assertEqual(
            filtered.measure_grains.grain_for("order_amt"),
            ("order_id",))

    def test_field_filter_hides_grain_column_but_keeps_identity(self):
        obj = make_object()
        filtered = self.context.op.field_filter(
            obj, keep=["region", "order_amt", "line_amt"])

        # Visible schema matches the request exactly...
        self.assertEqual(
            filtered.fields.visible_names(),
            ["region", "order_amt", "line_amt"])
        # ...but the grain identity survives, hidden, in the row layout.
        # Field order follows the source FieldList; only visibility changes.
        self.assertEqual(filtered.fields.names(),
                         ["order_id", "line_id", "region",
                          "order_amt", "line_amt"])
        self.assertEqual(
            [f.name for f in filtered.fields.hidden_fields()],
            ["order_id", "line_id"])
        first_row = next(iter(filtered.rows()))
        self.assertEqual(first_row[0], 1)

        result = self.context.op.aggregate(
            filtered, "region", measures=[("order_amt", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(rows["east"][1], 150.0)

    def test_drop_hides_grain_column(self):
        obj = make_object()
        filtered = self.context.op.field_filter(obj, drop=["order_id"])
        self.assertNotIn("order_id", filtered.fields.visible_names())
        self.assertIn("order_id", filtered.fields.names())
        result = self.context.op.aggregate(
            filtered, "region", measures=[("order_amt", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(rows["east"][1], 150.0)

    def test_rename_propagates_into_grains(self):
        obj = make_object()
        renamed = self.context.op.field_filter(
            obj, rename={"order_id": "order_no",
                         "order_amt": "total"})
        self.assertEqual(
            renamed.measure_grains.grain_for("total"), ("order_no",))

        result = self.context.op.aggregate(
            renamed, "region", measures=[("total", "sum")])
        rows = {r[0]: r for r in result.rows()}
        self.assertEqual(rows["east"][1], 150.0)

    def test_explicit_keep_makes_grain_visible(self):
        obj = make_object(grains={"order_amt": "order_id"})
        filtered = self.context.op.field_filter(
            obj, keep=["region", "order_id", "order_amt"])
        self.assertIn("order_id", filtered.fields.visible_names())
        self.assertEqual(filtered.fields.hidden_fields(), ())


class FieldFilterGrainProtectionTestCase(unittest.TestCase):
    def test_unknown_grain_field_raises(self):
        ff = FieldFilter(keep=["a"], grain_fields=["missing"])
        with self.assertRaises(NoSuchFieldError):
            ff.filter(FieldList(("a", "string")))

    def test_mask_keeps_protected_column(self):
        fields = FieldList(("oid", "integer"), ("amount", "number"))
        ff = FieldFilter(keep=["amount"], grain_fields=["oid"])
        self.assertEqual(ff.field_mask(fields), [True, True])
        out = ff.filter(fields)
        self.assertTrue(out.field("oid").hidden)
        self.assertFalse(out.field("amount").hidden)

    def test_field_hidden_attribute_roundtrip(self):
        field = Field("x", hidden=True)
        self.assertTrue(field.hidden)
        self.assertFalse(field.clone(hidden=False).hidden)
        self.assertTrue(field.to_dict()["hidden"])
        # Default fields are visible.
        self.assertFalse(Field("y").hidden)


class MeasureGrainsMetadataTestCase(unittest.TestCase):
    def test_prepare_accepts_forms(self):
        from_mapping = prepare_measure_grains({"amount": "oid"})
        from_list = prepare_measure_grains([("amount", ["oid"])])
        explicit = MeasureGrains([GrainMeasure("amount", ("oid",))])

        self.assertEqual(from_mapping.grain_for("amount"), ("oid",))
        self.assertEqual(from_list.grain_for("amount"), ("oid",))
        self.assertEqual(explicit.grain_for("amount"), ("oid",))
        self.assertIs(prepare_measure_grains(explicit), explicit)
        self.assertEqual(len(prepare_measure_grains(None)), 0)

    def test_prepare_rejects_garbage(self):
        with self.assertRaises(ArgumentError):
            prepare_measure_grains(42)

    def test_validate_reports_missing_fields(self):
        grains = prepare_measure_grains({"amount": "oid"})
        with self.assertRaises(NoSuchFieldError):
            grains.validate(FieldList(("amount", "number")))

    def test_renamed_preserves_unknown_names(self):
        grains = prepare_measure_grains({"amount": "oid"})
        renamed = grains.renamed({"amount": "total"})
        self.assertEqual(renamed.grain_for("total"), ("oid",))

    def test_merge_detects_contradiction(self):
        left = make_object(grains={"amount": "oid"})
        right = make_object(grains={"amount": "lid"})
        with self.assertRaises(ArgumentError):
            merge_measure_grains(left, right)

    def test_merge_unions_distinct_measures(self):
        left = make_object(grains={"order_amt": "order_id"})
        right = make_object(grains={"line_amt": "line_id"})
        merged = merge_measure_grains(left, right)
        self.assertEqual(merged.grain_for("order_amt"), ("order_id",))
        self.assertEqual(merged.grain_for("line_amt"), ("line_id",))


class RowsJoinGrainTestCase(unittest.TestCase):
    def setUp(self):
        self.context = OperationContext()
        self.context.add_operations_from(bubbles.ops.rows)

    def test_join_merges_grain_declarations(self):
        order_fields = FieldList(
            ("order_id", "integer"), ("region", "string"),
            ("order_amt", "number"))
        line_fields = FieldList(
            ("line_id", "integer"), ("order_id", "integer"),
            ("line_amt", "number"))
        orders = RowListDataObject(
            [[1, "east", 100.0], [2, "east", 50.0]], order_fields)
        lines = RowListDataObject(
            [[20, 2, 50.0]], line_fields)
        attach_measure_grains(orders, {"order_amt": "order_id"})
        attach_measure_grains(lines, {"line_amt": "line_id"})

        joined = self.context.op.join_details(
            orders, lines, "order_id", "order_id")

        self.assertEqual(
            joined.measure_grains.grain_for("order_amt"), ("order_id",))
        self.assertEqual(
            joined.measure_grains.grain_for("line_amt"), ("line_id",))


if __name__ == "__main__":
    unittest.main()
