import functools
from ...operation import RetryOperation
from ...prototypes import *
from ...metadata import Field, FieldList, FieldFilter
from ...metadata import prepare_aggregation_list, prepare_order_list
from ...objects import IterableDataSource, RowListDataObject
from ...errors import *
from ...grains import (GrainConflictError,
                       prepare_measure_grains, get_measure_grains,
                       merge_measure_grains)
from .utils import prepare_key, zip_condition, join_on_clause

try:
    import sqlalchemy
    from sqlalchemy import sql
except ImportError:
    from ...common import MissingPackage
    sqlalchemy = MissingPackage("sqlalchemy", "SQL streams", "http://www.sqlalchemy.org/",
                                comment = "Recommended version is > 0.7")
    sql = sqlalchemy

def _unary(func):
    @functools.wraps(func)
    def decorator(ctx, obj, *args, **kwargs):
        result = func(ctx, obj.sql_statement(), *args, **kwargs)
        return obj.clone_statement(statement=result)

    return decorator

#############################################################################
# Metadata Operations

@field_filter.register("sql")
def _(ctx, obj, keep=None, drop=None, rename=None, filter=None):
    """Returns a statement with fields according to the field filter.

    Grain-key fields referenced by the object's measure-grain declarations
    are protected: a selection that would drop them keeps the columns in the
    statement (and therefore in the row layout) but marks the corresponding
    fields hidden, so grain identities survive column filtering while the
    visible schema matches the requested fields exactly."""
    # TODO: preserve order of "keep" -> see FieldFilter

    if filter:
        if keep or drop or rename:
            raise OperationError("Either filter or keep, drop, rename should "
                                 "be used")
        field_filter = filter
    else:
        grain_fields = _sql_grain_field_names(obj)
        field_filter = FieldFilter(keep=keep, drop=drop, rename=rename,
                                   grain_fields=grain_fields)

    statement = obj.sql_statement()
    statement = statement.alias("__ff")

    columns = []

    # The mask keeps protected grain columns even when they are hidden.
    row_filter = field_filter.row_filter(obj.fields)

    for field in obj.fields:
        name = str(field)
        column = statement.c[name]
        if name in field_filter.rename:
            column = column.label(field_filter.rename[name])
        columns.append(column)

    selection = row_filter(columns)

    statement = sql.expression.select(selection, from_obj=statement)
    fields = field_filter.filter(obj.fields)

    result = obj.clone_statement(statement=statement, fields=fields)
    result.measure_grains = get_measure_grains(obj).renamed(
        field_filter.rename)
    return result


def _sql_grain_field_names(obj):
    """Collects every field name used as a grain key on `obj`."""
    names = []
    for declaration in get_measure_grains(obj).declarations():
        for name in declaration.grain:
            if name not in names:
                names.append(name)
    return names


#############################################################################
# Row Operations


@filter_by_value.register("sql")
def _(ctx, src, key, value, discard=False):
    """Returns difference between left and right statements"""

    if isinstance(key, (list, tuple)) and \
                    not isinstance(value, (list, tuple)):
        raise ArgumentError("If key is compound then value should be as well")

    cols = src.columns()
    statement = src.sql_statement()
    filter_cols = src.columns(prepare_key(key))
    if len(filter_cols) == 1:
        value = (value, )
    condition = zip_condition(filter_cols, value)
    statement = sql.expression.select(cols, from_obj=statement,
                                        whereclause=condition)

    statement = statement.alias("__value_filter")
    return src.clone_statement(statement=statement)


@filter_by_range.register("sql")
def _(ctx, src, field, low, high, discard=False):
    """Filter by range: field should be between low and high."""

    statement = src.sql_statement()
    key_column = statement.c[str(key)]

    if low is not None and high is None:
        cond = key_column >= low
    elif high is not None and low is None:
        cond = key_column < high
    else:
        cond = sql.expression.between(key_column, low, high)

    if discard:
        cond = sql.expression.not_(cond)

    statement = sql.expression.select(statement.columns,
                                      from_obj=statement,
                                      whereclause=cond)

    # TODO: remove this in newer SQLAlchemy version
    statement = statement.alias("__range_filter")
    return src.clone_statement(statement=statement)


@filter_by_set.register("sql")
def _(ctx, obj, key, value_set, discard=False):
    raise RetryOperation(["rows"], reason="Not implemented")


@filter_not_empty.register("sql")
def _(ctx, obj, field):
    statement = obj.sql_statement()

    column = statement.c[str(field)]
    condition = column != None
    selection = obj.columns()

    statement = sql.expression.select(selection, from_obj=statement,
                                            whereclause=condition)

    return obj.clone_statement(statement=statement)

@filter_by_predicate.register("sql")
def _(ctx, obj, fields, predicate, discard=False):
    raise RetryOperation(["rows"], reason="Not implemented")


@distinct.register("sql")
def _(ctx, obj, keys=None):
    """Returns a statement that selects distinct values for `keys`"""

    statement = obj.sql_statement()
    if keys:
        keys = prepare_key(keys)
    else:
        keys = obj.fields.names()

    cols = [statement.c[str(key)] for key in keys]
    statement = sql.expression.select(cols, from_obj=statement, group_by=cols)

    fields = obj.fields.fields(keys)

    return obj.clone_statement(statement=statement, fields=fields)


@first_unique.register("sql")
def _(ctx, statement, keys=None):
    """Returns a statement that selects whole rows with distinct values
    for `keys`"""
    # TODO: use prepare_key
    raise NotImplementedError

@sample.register("sql")
@_unary
def _(ctx, statement, value, mode="first"):
    """Returns a sample. `statement` is expected to be ordered."""

    if mode == "first":
        return statement.select(limit=value)
    else:
        raise RetryOperation(["rows"], reason="Unhandled mode '%s'" % mode)


@sort.register("sql")
@_unary
def _(ctx, statement, orderby):
    """Returns a ordered SQL statement. `orders` should be a list of
    two-element tuples `(field, order)`"""

    # Each attribute mentioned in the order should be present in the selection
    # or as some column from joined table. Here we get the list of already
    # selected columns and derived aggregates

    orderby = prepare_order_list(orderby)

    columns = []
    for field, order in orderby:
        column = statement.c[str(field)]
        order = order.lower()
        if order.startswith("asc"):
            column = column.asc()
        elif order.startswith("desc"):
            column = column.desc()
        else:
            raise ValueError("Unknown order %s for column %s") % (order, column)

        columns.append(column)

    statement = sql.expression.select(statement.columns,
                                   from_obj=statement,
                                   order_by=columns)
    return statement

aggregation_functions = {
    "sum": sql.functions.sum,
    "min": sql.functions.min,
    "max": sql.functions.max,
    "count": sql.functions.count
}


@aggregate.register("sql")
def _(ctx, obj, key, measures=None, include_count=True,
              count_field="record_count", measure_grains=None,
              grain_counts=None):

    """Aggregate `measures` by `key`.

    When no fact-grain declarations are present (neither in
    `measure_grains` nor attached to `obj`), the classic row-wise SQL
    aggregation is used unchanged.

    With grain declarations the aggregation becomes fact-grain aware and is
    expressed with window functions – the same deduplication technique
    mature BI engines use for joined fact tables: every grain measure is
    reduced to one value per surviving grain identity inside a subquery,
    then aggregated normally. Conflicting values for one identity are
    detected through a separate ``COUNT(DISTINCT ...)`` probe that raises
    :class:`GrainConflictError`. If the database cannot execute the window
    functions, the operation retries through the fully materialized row-wise
    implementation, whose result remains a natively consumable rows object.
    """

    # The metadata-level prepare_key tolerates a None key (whole-table
    # aggregation); the SQL backend's variant does not, so normalize here.
    keys = prepare_key(key) if key else ()

    if measures:
        measures = prepare_aggregation_list(measures)
    else:
        measures = []

    effective_grains = prepare_measure_grains(
        measure_grains if measure_grains is not None
        else get_measure_grains(obj))
    if effective_grains:
        effective_grains.validate(obj.fields)

    grain_counts = _prepare_sql_grain_counts(grain_counts, obj)

    if not effective_grains and not grain_counts:
        return _aggregate_sql_legacy(obj, keys, measures, include_count,
                                     count_field)

    try:
        statement_object = _aggregate_sql_grain_aware(
            ctx, obj, keys, measures, include_count, count_field,
            effective_grains, grain_counts)
    except GrainConflictError:
        # A semantic conflict must reach the caller verbatim instead of
        # being masked by the row-wise fallback.
        raise
    except (sqlalchemy.exc.SQLAlchemyError, NotImplementedError) as error:
        # The statement could not even be built with window functions:
        # fall back to the materialized row-wise implementation below.
        raise RetryOperation(["rows"],
                             reason="SQL grain aggregation unavailable "
                                    "(%s); using materialized row aggregation"
                                    % error)

    # Eagerly materialize the grouped result. This (a) makes a dialect that
    # rejects window functions fail inside this operation call, where the
    # retry below can catch it, and (b) returns a plain rows object that any
    # consumer can read natively. The expensive fan-out and dedup still
    # happen inside the database; only the compact grouped output is held.
    try:
        data = [tuple(row) for row in statement_object.rows()]
    except GrainConflictError:
        raise
    except Exception as error:
        # Drivers surface unsupported window functions through different
        # exception hierarchies, so any execution failure is retried rather
        # than trusted to be a particular SQLAlchemy error class.
        raise RetryOperation(["rows"],
                             reason="SQL grain aggregation could not be "
                                    "executed (%s); using materialized row "
                                    "aggregation" % error)

    return RowListDataObject(data, fields=statement_object.fields)


def _prepare_sql_grain_counts(grain_counts, obj):
    if not grain_counts:
        return []

    result = []
    for item in grain_counts:
        if isinstance(item, (list, tuple)):
            output_name, grain = item
        else:
            grain = item
            output_name = "%s_count" % str(grain)
        grain_key = prepare_key(grain)
        for name in grain_key:
            if name not in obj.fields.names():
                raise NoSuchFieldError(
                    "Grain count field '%s' is not a field of the object"
                    % name)
        result.append((output_name, grain_key))
    return result


def _aggregate_sql_legacy(obj, keys, measures, include_count, count_field):
    """The original SQL aggregation, unchanged for objects without grain
    declarations."""

    out_fields = FieldList()
    if keys:
        out_fields += obj.fields.fields(keys)
    out_fields += obj.fields.aggregated_fields(
        measures, include_count, count_field)

    statement = obj.sql_statement()
    group = [statement.c[str(key)] for key in keys]

    selection = [statement.c[str(key)] for key in keys]

    for measure, agg_name in measures:
        func = aggregation_functions[agg_name]
        label = "%s_%s" % (str(measure), agg_name)
        aggregation = func(obj.column(measure)).label(label)
        selection.append(aggregation)

    if include_count:
        count = sql.functions.count(1).label(count_field)
        selection.append(count)

    statement = sql.expression.select(selection,
                                      from_obj=statement,
                                      group_by=group)

    return obj.clone_statement(statement=statement, fields=out_fields)


def _aggregate_sql_grain_aware(ctx, obj, keys, measures, include_count,
                               count_field, measure_grains, grain_counts):
    statement = obj.sql_statement().alias("__ga_src")

    # Split measures into row-wise (no declared grain) and grain-aware ones.
    row_measures = []
    grain_measures = []
    for name, agg_name in measures:
        grain = measure_grains.grain_for(name)
        if grain is None:
            row_measures.append((name, agg_name))
        else:
            if agg_name not in ("sum", "min", "max", "average", "count"):
                raise ArgumentError(
                    "Grain-aware aggregation '%s' is not supported for "
                    "measure '%s'" % (agg_name, name))
            grain_measures.append((name, agg_name, grain))

    _probe_grain_conflicts(obj, statement, keys, grain_measures)

    group_cols = [statement.c[str(key)] for key in keys]

    # Inner select: group keys, raw measure columns, plus one row-number
    # column per grain measure/grain count. The row number is partitioned by
    # the output group plus the grain identity, so rn == 1 marks the single
    # surviving fact per identity. Row-wise measures keep their raw row
    # values and are aggregated in the outer select, matching the legacy
    # fan-out semantics for measures without a declared grain.
    inner_selection = list(group_cols)

    # Every measure column is passed through so the outer select can
    # reference it by name, whether or not the measure is grain aware.
    for name, agg_name in measures:
        inner_selection.append(statement.c[str(name)])

    # Window PARTITION BY lists. The grouping keys are always part of the
    # partition so identities are counted within each output group.
    def partition_columns(grain):
        return group_cols + [statement.c[str(g)] for g in grain]

    # Metadata for the generated row-number columns, consumed when building
    # the outer expressions.
    dedup_labels = []

    for name, agg_name, grain in grain_measures:
        measure_col = statement.c[str(name)]
        partition = partition_columns(grain)
        # Include the aggregate in the label so one measure aggregated
        # several times (sum + average + ...) does not collide on one
        # row-number column name.
        rn_label = "__rn_%s_%s" % (name, agg_name)
        # Non-NULL values sort first, so the surviving row carries a value
        # whenever the fact has one; equal values are indistinguishable,
        # which is the desired behaviour.
        row_number = sqlalchemy.func.row_number().over(
            partition_by=partition,
            order_by=measure_col.desc())
        inner_selection.append(row_number.label(rn_label))
        dedup_labels.append(("measure", name, agg_name, grain, rn_label))

    for output_name, grain in grain_counts:
        partition = partition_columns(grain)
        rn_label = "__rn_count_%s" % "_".join(grain)
        first_grain_col = statement.c[str(grain[0])]
        row_number = sqlalchemy.func.row_number().over(
            partition_by=partition,
            order_by=first_grain_col.asc())
        inner_selection.append(row_number.label(rn_label))
        dedup_labels.append(("count", output_name, grain, rn_label, None))

    inner = sql.expression.select(inner_selection,
                                  from_obj=statement).alias("__ga_dedup")

    outer_selection = [inner.c[str(key)] for key in keys]
    out_fields = FieldList()
    if keys:
        out_fields += obj.fields.fields(keys)

    # Row-wise measures first, in original measure order. To keep output
    # ordering identical to the rows backend we remember the position.
    ordered_outputs = []
    for name, agg_name in measures:
        grain = measure_grains.grain_for(name)
        if grain is None:
            ordered_outputs.append(("row", name, agg_name))
        else:
            ordered_outputs.append(("grain", name, agg_name))

    for output_name, grain in grain_counts:
        ordered_outputs.append(("grain_count", output_name, grain))

    # Build outer expressions.
    for kind, *payload in ordered_outputs:
        if kind == "row":
            name, agg_name = payload
            label = "%s_%s" % (name, agg_name)
            func = aggregation_functions[agg_name]
            outer_selection.append(func(inner.c[name]).label(label))
            field = obj.fields.field(name)
            out_fields.append(field.clone(name=label,
                                           analytical_type="measure"))
        elif kind == "grain":
            name, agg_name = payload
            label = "%s_%s" % (name, agg_name)
            rn_label = "__rn_%s_%s" % (name, agg_name)
            measure_col = inner.c[name]
            rn_col = inner.c[rn_label]
            surviving = sql.expression.case(
                [(rn_col == 1, measure_col)], else_=None)

            if agg_name == "sum":
                expr = sqlalchemy.func.sum(surviving)
            elif agg_name == "min":
                expr = sqlalchemy.func.min(surviving)
            elif agg_name == "max":
                expr = sqlalchemy.func.max(surviving)
            elif agg_name == "average":
                expr = sqlalchemy.func.avg(surviving)
            elif agg_name == "count":
                expr = sqlalchemy.func.sum(
                    sql.expression.case([(rn_col == 1, 1)], else_=0))
            outer_selection.append(expr.label(label))
            field = obj.fields.field(name)
            out_fields.append(field.clone(name=label,
                                           analytical_type="measure"))
        else:
            output_name, grain = payload
            rn_label = "__rn_count_%s" % "_".join(grain)
            rn_col = inner.c[rn_label]
            expr = sqlalchemy.func.sum(
                sql.expression.case([(rn_col == 1, 1)], else_=0))
            outer_selection.append(expr.label(output_name))
            out_fields.append(Field(output_name,
                                    storage_type="integer",
                                    analytical_type="measure"))

    if include_count:
        outer_selection.append(
            sqlalchemy.func.count(1).label(count_field))
        out_fields.append(Field(count_field,
                                storage_type="integer",
                                analytical_type="measure"))

    outer_group = [inner.c[str(key)] for key in keys]
    final_statement = sql.expression.select(outer_selection,
                                            from_obj=inner,
                                            group_by=outer_group)

    return obj.clone_statement(statement=final_statement,
                               fields=out_fields)


def _probe_grain_conflicts(obj, statement, keys, grain_measures):
    """Raises GrainConflictError when a grain identity maps to more than one
    distinct non-NULL measure value within one aggregation group.

    The probe is one extra grouped query per grain measure and is executed
    eagerly so conflicts surface as an explicit error rather than as silent
    over-counting.
    """

    group_cols = [statement.c[str(key)] for key in keys]

    for name, agg_name, grain in grain_measures:
        measure_col = statement.c[str(name)]
        grain_cols = [statement.c[str(g)] for g in grain]
        distinct = sqlalchemy.func.count(
            sqlalchemy.func.distinct(measure_col))
        probe = sql.expression.select(
            group_cols + grain_cols + [distinct.label("__distinct")],
            from_obj=statement,
            whereclause=measure_col != None,
            group_by=group_cols + grain_cols,
            having=distinct > 1,
            limit=1)
        rows = obj.store.execute(probe).fetchall()
        if rows:
            offending = rows[0]
            identity = tuple(offending[len(keys) + i]
                             for i in range(len(grain)))
            raise GrainConflictError(
                measure=name,
                grain=grain,
                identity=identity,
                values="multiple distinct values")


#############################################################################
# Field Operations

@append_constant_fields.register("sql")
def _(ctx, obj, fields, values):
    statement = obj.sql_statement()

    new_fields = obj.fields + FieldList(*fields)

    selection = statement.c
    selection += values

    statement = sql.expression.select(selection, from_obj=statement)
    result = obj.clone_statement(statement=statement, fields=new_fields)

    return result

@dates_to_dimension.register("sql")
def _(ctx, obj, fields=None, unknown_date=0):
    """Update all date fields to be date IDs. `unknown_date` is a key to date
    dimension table for unspecified date (NULL in the source).
    `fields` is list of date fields. If not specified, then all date fields
    are considered."""

    statement = obj.sql_statement()
    statement = statement.alias("__to_dim")

    if fields:
        date_fields = obj.fields(fields)
    else:
        date_fields = obj.fields.fields(storage_type="date")

    selection = []
    fields = []
    for field in obj.fields:
        col = statement.c[str(field)]
        if field in date_fields:
            # Safe way
            year = sql.func.extract("year", col)
            month = sql.func.extract("month", col)
            day = sql.func.extract("day", col)
            key = year * 10000 + month * 100 + day
            col = sql.expression.case([ (col != None, key) ],
                    else_=unknown_date)

            # print("CAST TO: %s" % str(col))

            col = col.label(field.name)
            fields.append(field.clone(storage_type="integer",
                                      concrete_storage_type=None))
        else:
            fields.append(field.clone())

        selection.append(col)

    fields = FieldList(*fields)
    statement = sql.expression.select(selection,
                                      from_obj=statement)

    # TODO: mark date fields to be integers
    return obj.clone_statement(statement=statement, fields=fields)

@split_date.register("sql")
def _(ctx, obj, fields, parts=["year", "month", "day"]):
    """Extract `parts` from date objects replacing the original date field
    with parts field."""

    statement = obj.sql_statement()
    date_fields = prepare_key(fields)

    # Validate date fields
    for f in obj.fields.fields(date_fields):
        if f.storage_type != "date":
            raise FieldError("Field '%s' is not a date field" % f)

    # Prepare output fields
    fields = FieldList()
    proto = Field(name="p", storage_type="integer", analytical_type="ordinal")

    selection = []

    for field in obj.fields:
        name = str(field)
        col = statement.c[name]
        if name in date_fields:
            for part in parts:
                label = "%s_%s" % (str(field), part)
                fields.append(proto.clone(name=label))
                col_part = sql.expression.extract(part, col)
                col_part = col_part.label(label)
                selection.append(col_part)
        else:
            fields.append(field.clone())
            selection.append(col)

    statement = sql.expression.select(selection,
                                      from_obj=statement)

    return obj.clone_statement(statement=statement, fields=fields)

# @operation("sql")
# def string_to_date(ctx, obj, fields, fmt="%Y-%m-%dT%H:%M:%S.Z"):
#
#     date_fields = prepare_key(fields)
#
#     selection = []
#     # Prepare output fields
#     fields = FieldList()
#     for field in obj.fields:
#         col = statement.c[str(field)]
#         if str(field) in date_fields:
#             fields.append(field.clone(storage_type="date",
#                                       concrete_storage_type=None))
#
#         else:
#             fields.append(field.clone())
#         selection.append(col)
#
#     return IterableDataSource(iterator(indexes), fields)


#############################################################################
# Compositions

@append.register("sql[]")
def _(ctx, objects):
    """Returns a statement with sequentialy concatenated results of the
    `statements`. Statements are chained using ``UNION``."""
    first = objects[0]

    if not all(first.can_compose(o) for o in objects[1:]):
        raise RetryOperation(["rows", "rows[]"],
                             reason="Can not compose")

    statements = [o.sql_statement() for o in objects]
    statement = sqlalchemy.sql.expression.union(*statements)

    return first.clone_statement(statement=statement)


@join_details.register("sql", "sql")
def _(ctx, master, detail, master_key, detail_key):
    """Creates a master-detail join using simple or composite keys. The
    columns used as a key in the `detail` object are not included in the
    result.
    """
    # TODO: add left inner, left outer

    if not master.can_compose(detail):
        raise RetryOperation(["rows", "rows"], reason="Can not compose")

    master_key = prepare_key(master_key)
    detail_key = prepare_key(detail_key)

    master_stat = master.sql_statement().alias("__m")
    detail_stat = detail.sql_statement().alias("__d")

    # Prepare the ON left=right ... clause
    onclause = join_on_clause(master_stat, detail_stat, master_key, detail_key)

    # Prepare output fields and columns selection - the selection skips detail
    # columns that are used as key, because they are already present in the
    # master table.

    out_fields = master.fields.clone()
    selection = list(master_stat.columns)
    for field in detail.fields:
        if str(field) not in detail_key:
            out_fields.append(field)
            selection.append(detail_stat.c[str(field)])

    joined = sql.expression.join(master_stat,
                                 detail_stat,
                                 onclause=onclause)

    # Alias the output fields to match the field names
    aliased = []
    for col, field in zip(selection, out_fields):
        aliased.append(col.label(field.name))

    select = sql.expression.select(aliased,
                                from_obj=joined,
                                use_labels=True)

    result = master.clone_statement(statement=select, fields=out_fields)
    # The join fans master rows out but preserves every fact of both sides,
    # so their grain declarations stay valid and are combined.
    result.measure_grains = merge_measure_grains(master, detail)
    _validate_sql_grain_fields(result)
    return result


def _validate_sql_grain_fields(obj):
    """Checks that every field referenced by grain declarations is present
    in the resulting object."""
    names = set(obj.fields.names())
    for declaration in get_measure_grains(obj).declarations():
        if declaration.measure not in names:
            raise NoSuchFieldError(
                "Grain measure '%s' was lost during the operation"
                % declaration.measure)
        for name in declaration.grain:
            if name not in names:
                raise NoSuchFieldError(
                    "Grain key field '%s' of measure '%s' was lost during "
                    "the operation" % (name, declaration.measure))

# TODO: deprecated
@join_details.register("sql", "sql[]", name="join_details")
def _(ctx, master, details, joins):
    """Creates left inner master-detail join (star schema) where `master` is an
    iterator if the "bigger" table `details` are details. `joins` is a list of
    tuples `(master, detail)` where the master is index of master key and
    detail is index of detail key to be matched.

    If `inner` is `True` then inner join is performed. That means that only
    rows from master that have corresponding details are returned.

    .. warning::

        all detail iterators are consumed and result is held in memory. Do not
        use for large datasets.
    """
    # TODO: update documentation

    if not details:
        raise ArgumentError("No details provided, nothing to join")

    if not joins:
        raise ArgumentError("No joins specified")

    if len(details) != len(joins):
        raise ArgumentError("For every detail there should be a join "
                            "(%d:%d)." % (len(details), len(joins)))

    if not all(master.can_compose(detail) for detail in details):
        raise RetryOperation(["rows", "rows[]"], reason="Can not compose")

    out_fields = master.fields.clone()

    master_stmt = master.sql_statement().alias("master")
    selection = list(master_stmt.columns)

    joined = master_stmt
    i = 0
    for detail, join in zip(details, joins):
        alias = "detail%s" % i
        det_stmt = detail.sql_statement().alias(alias)
        master_key = join["master"]
        detail_key = join["detail"]

        onclause = master_stmt.c[master_key] == det_stmt.c[detail_key]
        # Skip detail key in the output

        for field, col in zip(detail.fields, det_stmt.columns):
            if str(field) != str(detail_key):
                selection.append(col)
                out_fields.append(field.clone())

        joined = sql.expression.join(joined,
                                     det_stmt,
                                     onclause=onclause)

    aliased = []
    for col, field in zip(selection, out_fields):
        aliased.append(col.label(field.name))

    select = sql.expression.select(aliased,
                                from_obj=joined,
                                use_labels=True)

    result = master.clone_statement(statement=select, fields=out_fields)
    merged = get_measure_grains(master)
    for detail in details:
        merged = merge_measure_grains(merged, detail)
    result.measure_grains = merged
    _validate_sql_grain_fields(result)
    return result


@added_keys.register("sql", "sql")
def _(ctx, src, target, src_key, target_key=None):
    """Returns difference between left and right statements"""

    # FIXME: add composition checking

    if not src.can_compose(target):
        raise RetryOperation(("rows", "sql"), reason="Can not compose")

    target_key = target_key or src_key

    src_cols = src.columns(prepare_key(src_key))
    target_cols = target.columns(prepare_key(target_key))

    src_stat = src.sql_statement()
    target_stat = target.sql_statement()

    src_selection = sql.expression.select(src_cols, from_obj=src_stat)
    target_selection = sql.expression.select(target_cols, from_obj=target_stat)

    diff = src_selection.except_(target_selection)

    return src.clone_statement(statement=diff)


@added_rows.register("sql", "sql")
def _(ctx, src, target, src_key, target_key=None):
    diff = ctx.added_keys(src, target, src_key, target_key)

    diff_stmt = diff.sql_statement()
    diff_stmt = diff_stmt.alias("__added_keys")
    src_stmt = src.sql_statement()

    key = prepare_key(src_key)

    src_cols = src.columns(key)
    diff_cols = [diff_stmt.c[f] for f in key]

    cond = zip_condition(src_cols, diff_cols)

    join = sql.expression.join(src_stmt, diff_stmt, onclause=cond)
    join = sql.expression.select(src.columns(), from_obj=join)

    return src.clone_statement(statement=join)


@added_rows.register("rows", "sql", name="added_rows")
def _(ctx, src, target, src_key, target_key=None):

    src_key = prepare_key(src_key)

    if target_key:
        target_key = prepare_key(target_key)
    else:
        target_key = src_key

    statement = target.sql_statement()
    target_cols = target.columns(target_key)

    field_filter = FieldFilter(keep=src_key).row_filter(src.fields)

    def iterator():
        for row in src.rows():
            row_key = field_filter(row)

            cond = zip_condition(target_cols, row_key)

            select = sql.expression.select([sql.func.count(1)],
                                           from_obj=statement,
                                           whereclause=cond)

            result = target.store.execute(select)
            result = list(result)
            if len(result) >= 1 and result[0][0] == 0:
                yield row

    return IterableDataSource(iterator(), fields=src.fields)


# TODO: dimension loading: new values
# TODO: create decorator @target that will check first argument whether it is
# a target or not

@changed_rows.register("sql", "sql")
def _(ctx, dim, source, dim_key, source_key, fields, version_field):
    """Return an object representing changed dimension rows.

    Arguments:
        * `dim` – dimension table (target)
        * `source` – source statement
        * `dim_key` – dimension table key
        * `source_key` – source table key
        * `fields` – fields to be compared for changes
        * `version_field` – field that is optionally checked to be empty (NULL)

    """
    src_columns = source.columns(fields)
    dim_columns = dim.columns(fields)
    src_stmt = source.sql_statement()
    dim_stmt = dim.sql_statement()

    # 1. find changed records
    # TODO: this might be a separate operation

    join_cond = zip_condition(dim.columns(prepare_key(dim_key)),
                              source.columns(prepare_key(source_key)))
    join = sql.expression.join(src_stmt, dim_stmt, onclause=join_cond)

    change_cond = [ d != s for d, s in zip(dim_columns, src_columns) ]
    change_cond = sql.expression.or_(*change_cond)

    if version_field:
        version_column = dim.column(version_field)
        change_cond = sql.expression.and_(change_cond, version_column == None)

    join = sql.expression.select(source.columns(),
                                 from_obj=join,
                                 whereclause=change_cond)

    return source.clone_statement(statement=join)

#############################################################################
# Loading

# TODO: continue here
@load_versioned_dimension.register("sql_table", "sql")
def _(ctx, dim, source, dim_key, fields,
                             version_fields=None, source_key=None):
    """Type 2 dimension loading."""
    # Now I need to stay in the same kernel!

    new = added_rows(dim, source)


#############################################################################
# Auditing


@count_duplicates.register("sql")
def _(ctx, obj, keys=None, threshold=1,
                       record_count_label="record_count"):
    """Returns duplicate rows based on `keys`. `threshold` is lowest number of
    duplicates that has to be present to be returned. By default `threshold`
    is 1. If no keys are specified, then all columns are considered."""

    if not threshold or threshold < 1:
        raise ValueError("Threshold should be at least 1 "
                         "meaning 'at least one duplcate'.")

    statement = obj.sql_statement()

    count_field = Field(record_count_label, "integer")

    if keys:
        keys = prepare_key(keys)
        group = [statement.c[str(field)] for field in keys]
        fields = list(keys)
        fields.append(count_field)
        out_fields = FieldList(*fields)
    else:
        group = list(statement.columns)
        out_fields = obj.fields.clone() + FieldList(count_field)

    counter = sqlalchemy.func.count("*").label(record_count_label)
    selection = group + [counter]
    condition = counter > threshold

    statement = sql.expression.select(selection,
                                   from_obj=statement,
                                   group_by=group,
                                   having=condition)

    result = obj.clone_statement(statement=statement, fields=out_fields)
    return result

@duplicate_stats.register("sql_statement")
def _(ctx, obj, fields=None, threshold=1):
    """Return duplicate statistics of a `statement`"""
    count_label = "__record_count"
    dups = duplicates(obj, threshold, count_label)
    statement = dups.statement
    statement = statement.alias("duplicates")

    counter = sqlalchemy.func.count("*").label("record_count")
    group = statement.c[count_label]
    result_stat = sqlalchemy.sql.expression.select([counter, group],
                                              from_obj=statement,
                                              group_by=[group])

    fields = dups.fields.clone()
    fields.add(count_label)

    result = obj.clone_statement(statement=result_stat, fields=fields)
    return result

@nonempty_count.register("sql")
def _(ctx, obj, fields=None):
    """Return count of empty fields for the object obj"""

    # FIXME: add fields
    # FIXME: continue here

    statement = obj.sql_statement()
    statement = statement.alias("empty")

    if not fields:
        fields = obj.fields
    fields = prepare_key(fields)

    cols = [statement.c[f] for f in fields]
    selection = [sqlalchemy.func.count(col) for col in cols]
    statement = sqlalchemy.sql.expression.select(selection,
                                                  from_obj=statement)

    out_fields = obj.fields.fields(fields)
    return obj.clone_statement(statement=statement, fields=out_fields)

    # field, key, key, key, empty_count

@distinct_count.register("sql")
def _(ctx, obj, fields=None):
    """Return count of empty fields for the object obj"""

    # FIXME: add fields
    # FIXME: continue here

    statement = obj.sql_statement()
    statement = statement.alias("distinct_count")

    if not fields:
        fields = obj.fields
    fields = prepare_key(fields)

    cols = [statement.c[f] for f in fields]
    selection = []
    for col in cols:
        c = sqlalchemy.func.count(sqlalchemy.func.distinct(col))
        selection.append(c)

    statement = sqlalchemy.sql.expression.select(selection,
                                                  from_obj=statement)

    out_fields = obj.fields.fields(fields)
    return obj.clone_statement(statement=statement, fields=out_fields)


#############################################################################
# Assertions


@assert_unique.register("sql")
def _(ctx, obj, key=None):
    """Checks whether the receiver has unique values for `key`. If `key` is
    not specified, then all fields from `obj` are considered."""

    statement = obj.sql_statement().alias("__u")

    if key:
        key = prepare_key(key)
        group = [statement.c[field] for field in key]
    else:
        group = list(statement.columns)

    counter = sqlalchemy.func.count("*").label("duplicate_count")
    selection = [counter]
    condition = counter > 1

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       group_by=group,
                                       having=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))

    if len(result) != 0:
        raise ProbeAssertionError

    return obj

@assert_contains.register("sql")
def _(ctx, obj, field, value):
    statement = obj.sql_statement().alias("__u")
    column = statement.c[str(field)]

    condition = column == value
    selection = [1]

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       whereclause=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))
    if len(result) != 1:
        raise ProbeAssertionError

    return obj

@assert_missing.register("sql")
def _(ctx, obj, field, value):
    # statement = obj.sql_statement().alias("__u")
    statement = obj.sql_statement()
    column = statement.c[str(field)]

    condition = column == value
    selection = [1]

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       whereclause=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))
    if len(result) != 0:
        raise ProbeAssertionError

    return obj

#############################################################################
# Conversions


@as_records.register("sql")
def _(ctx, obj):
    """Return object with records representation."""
    # SQL Alchemy result can be used as both - records or rows, so we just
    # return the object:

    return obj


#############################################################################
# Loading

@insert.register("sql", "sql")
def _(ctx, source, target):
    if not target.can_compose(src):
        raise RetryOperation(["rows"])

    # Flush all data that were added through append() to preserve
    # insertion order (just in case)
    target.flush()

    # Prepare INSERT INTO ... SELECT ... statement
    statement = InsertFromSelect(target.table, source.selectable())

    target.store.execute(statement)

    return target

@insert.register("rows", "sql")
def _(ctx, source, target):

    if len(source.fields) > len(target.fields):
         raise OperationError("Number of source fields %s is greater than "
                              "number of target fields %s" % (len(source.fields),
                                                             len(target.fields)))

    missing = set(source.fields.names()) - set(target.fields.names())
    if missing:
        raise OperationError("Source contains fields that are not in the "
                "target: %s" % (missing, ))

    indexes = []
    for name in target.fields.names():
        if name in source.fields:
            indexes.append(source.fields.index(name))
        else:
            indexes.append(None)

    target.flush()
    for row in source.rows():
        row = [row[i] if i is not None else None for i in indexes]
        target.append(row)

    target.flush()

    return target
