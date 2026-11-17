# -*- coding: utf-8 -*-
"""Fact-grain aware aggregation support.

When a master (fact) table is joined with detail tables, one master row is
repeated for every matching detail row. A plain row-wise aggregation of a
master-level measure therefore counts the measure multiple times – for
example an order amount is summed once per order line.

The remedy implemented here is to declare, per measure, a *grain key*: a
non-NULL field (or a tuple of fields) that identifies one fact of the
grain at which the measure is recorded. An order-level measure declares
the order key, a detail-level measure declares the detail key. During
aggregation the measure is then contributed once per *surviving* grain
identity within each group, instead of once per physical row.

The machinery is deliberately backend agnostic. A :class:`MeasureGrains`
instance travels with data objects (``obj.measure_grains``) through
native joins, row filters and :class:`~bubbles.metadata.FieldFilter`
column filtering/renaming, so a grain declared on a source object keeps
working on the objects derived from it. Hiding a grain-key column through
``field_filter`` does not destroy the identity: the column is retained in
the row layout and merely marked as hidden on its field.
"""

from .errors import ArgumentError, NoSuchFieldError, GrainConflictError
from .metadata import prepare_key

__all__ = [
    "MeasureGrains",
    "GrainMeasure",
    "prepare_measure_grains",
    "get_measure_grains",
    "attach_measure_grains",
    "merge_measure_grains",
    "GrainAccumulator",
]


class GrainMeasure(object):
    """Declaration of the fact grain of one measure.

    Attributes:

    * `measure` – name of the measure field the declaration applies to
    * `grain` – tuple of non-NULL field names forming the grain key that
      identifies one fact at the measure's grain
    """

    __slots__ = ("measure", "grain")

    def __init__(self, measure, grain):
        self.measure = str(measure)
        self.grain = tuple(str(f) for f in grain)

    def __repr__(self):
        return "GrainMeasure(%r, %r)" % (self.measure, self.grain)

    def __eq__(self, other):
        if not isinstance(other, GrainMeasure):
            return False
        return self.measure == other.measure and self.grain == other.grain

    def __ne__(self, other):
        return not self.__eq__(other)


class MeasureGrains(object):
    """Maps measure names to their fact grain.

    A measure grains object is a metadata annotation of a data object. It
    is independent of field visibility: a grain-key column may be hidden by
    a field filter while the declaration keeps referring to it.
    """

    def __init__(self, declarations=None):
        """Creates measure grains from `declarations`, which is a list of
        :class:`GrainMeasure` objects or `(measure, grain_key)` tuples."""

        # A plain dict preserves insertion order on all supported Python
        # versions (3.7+), which keeps declaration order deterministic.
        self._declarations = {}
        for declaration in declarations or []:
            if isinstance(declaration, GrainMeasure):
                gm = declaration
            elif isinstance(declaration, (list, tuple)):
                measure, grain = declaration
                gm = GrainMeasure(measure, prepare_key(grain))
            else:
                raise ArgumentError("Invalid measure grain declaration: %r"
                                    % (declaration, ))
            self._declarations[gm.measure] = gm

    def grain_for(self, measure):
        """Returns the grain key tuple for `measure` or ``None`` when the
        measure has no declared grain."""
        declaration = self._declarations.get(str(measure))
        if declaration is None:
            return None
        return declaration.grain

    def measures(self):
        """Returns names of measures with a declared grain."""
        return list(self._declarations.keys())

    def declarations(self):
        return list(self._declarations.values())

    def __contains__(self, measure):
        return str(measure) in self._declarations

    def __len__(self):
        return len(self._declarations)

    def __bool__(self):
        return bool(self._declarations)

    def copy(self):
        return MeasureGrains(self._declarations.values())

    def renamed(self, rename):
        """Returns a copy with field names mapped through the `rename`
        dictionary. Measures and grain keys unknown to the rename map keep
        their names, which makes the object safe to reuse after a field
        filter that only renames a subset of fields."""

        result = MeasureGrains()
        for declaration in self._declarations.values():
            measure = rename.get(declaration.measure, declaration.measure)
            grain = tuple(rename.get(name, name) for name in declaration.grain)
            result._declarations[measure] = GrainMeasure(measure, grain)
        return result

    def validate(self, fields):
        """Raises :class:`~bubbles.errors.NoSuchFieldError` when a declared
        field is missing from `fields`."""

        names = set(fields.names())
        for declaration in self._declarations.values():
            if declaration.measure not in names:
                raise NoSuchFieldError("Grain measure '%s' is not a field "
                                       "of the object" % declaration.measure)
            for name in declaration.grain:
                if name not in names:
                    raise NoSuchFieldError("Grain key field '%s' of measure "
                                           "'%s' is not a field of the "
                                           "object" %
                                           (name, declaration.measure))

    def __repr__(self):
        return "MeasureGrains(%r)" % (list(self._declarations.values()), )


def OrderedDict_like():
    # Imported lazily at module level would create a cycle risk in some
    # packaging layouts; a plain dict is enough – Python 3.7+ preserves
    # insertion order anyway.
    return {}


def prepare_measure_grains(measure_grains):
    """Coerces an arbitrary measure-grain specification into a
    :class:`MeasureGrains` object.

    Accepted forms:

    * ``None`` or an empty list – an empty :class:`MeasureGrains`
    * a :class:`MeasureGrains` instance – returned unchanged
    * a mapping ``{measure: grain_key}``
    * a list of ``(measure, grain_key)`` tuples or :class:`GrainMeasure`
      objects, where a grain key may be a single field name or a list/tuple
      of field names
    """

    if measure_grains is None:
        return MeasureGrains()
    if isinstance(measure_grains, MeasureGrains):
        return measure_grains
    if isinstance(measure_grains, dict):
        declarations = list(measure_grains.items())
    elif isinstance(measure_grains, (list, tuple)):
        declarations = measure_grains
    else:
        raise ArgumentError("Invalid measure grains specification: %r"
                            % (measure_grains, ))

    return MeasureGrains(declarations)


def get_measure_grains(obj):
    """Returns the :class:`MeasureGrains` attached to `obj`, or an empty
    one when the object carries no grain declarations.

    A bare :class:`MeasureGrains` instance is accepted as well, which lets
    the helper be used in accumulation/merge loops."""

    if isinstance(obj, MeasureGrains):
        return obj
    grains = getattr(obj, "measure_grains", None)
    if grains is None:
        return MeasureGrains()
    return grains


def attach_measure_grains(obj, measure_grains):
    """Attaches `measure_grains` (coerced through
    :func:`prepare_measure_grains`) to `obj` and returns the object."""

    obj.measure_grains = prepare_measure_grains(measure_grains)
    return obj


def merge_measure_grains(left, right):
    """Merges grain declarations of two objects joined together.

    Declarations for the same measure must agree on the grain key,
    otherwise an :class:`~bubbles.errors.ArgumentError` is raised. Returns
    a new :class:`MeasureGrains` covering both sides of the join.
    """

    left = get_measure_grains(left)
    right = get_measure_grains(right)

    merged = left.copy()
    for declaration in right.declarations():
        existing = merged.grain_for(declaration.measure)
        if existing is not None and existing != declaration.grain:
            raise ArgumentError(
                "Conflicting grain declarations for measure '%s': "
                "%s versus %s" %
                (declaration.measure, existing, declaration.grain))
        merged._declarations[declaration.measure] = declaration
    return merged


class GrainAccumulator(object):
    """Pure-Python accumulation state for one aggregation group.

    The accumulator implements the fact-grain semantics required by the
    row-wise ``aggregate`` operation:

    * a measure with a declared grain contributes once per distinct,
      non-NULL grain identity actually present in the group
    * two rows with the same grain identity but different non-NULL measure
      values raise :class:`~bubbles.errors.GrainConflictError`; identities
      whose value is NULL on every row simply do not contribute
    * a measure without a declared grain accumulates row-wise (the legacy
      behaviour), but NULL values are ignored and an all-NULL result stays
      NULL instead of becoming zero
    * grain counts count distinct identities, including identities whose
      measure value is NULL
    """

    def __init__(self, row_measures, grain_measures, grain_counts=None):
        """`row_measures` is a list of `(name, index, aggregate)` tuples
        aggregated the legacy way. `grain_measures` is a list of
        `(name, measure_index, grain_indexes, aggregate)` tuples aggregated
        per grain identity. `grain_counts` is a list of
        `(output_name, grain_indexes)` tuples."""

        self.row_measures = row_measures
        self.grain_measures = grain_measures
        self.grain_counts = grain_counts or []

        # Legacy row-wise aggregates: NULL-aware state
        self._row_state = []
        for name, index, aggregate in row_measures:
            self._row_state.append(_new_row_state(aggregate))

        # Per grain-measure: {identity: value-or-NULL}
        self._grain_values = [{} for _ in grain_measures]

        # Per counted grain: set of identities
        self._count_identities = [set() for _ in self.grain_counts]

        self.record_count = 0

    def add_row(self, row):
        self.record_count += 1

        for i, (name, index, aggregate) in enumerate(self.row_measures):
            value = row[index]
            state = self._row_state[i]
            _update_row_state(state, aggregate, value)

        for i, (name, measure_index, grain_indexes, aggregate) in \
                enumerate(self.grain_measures):
            identity = tuple(row[pos] for pos in grain_indexes)
            value = row[measure_index]
            seen = self._grain_values[i]

            if identity in seen:
                previous = seen[identity]
                if value is not None and previous is not None \
                        and previous != value:
                    raise GrainConflictError(
                        measure=name,
                        grain=grain_indexes,
                        identity=identity,
                        values=[previous, value])
                if previous is None and value is not None:
                    seen[identity] = value
            else:
                seen[identity] = value

        for i, (output_name, grain_indexes) in enumerate(self.grain_counts):
            identity = tuple(row[pos] for pos in grain_indexes)
            self._count_identities[i].add(identity)

    def results(self):
        """Returns the tuple `(row_measure_results, grain_measure_results,
        grain_count_results, record_count)` for the group."""

        row_results = []
        for state, (name, index, aggregate) in \
                zip(self._row_state, self.row_measures):
            row_results.append(_finalize_row_state(state, aggregate))

        grain_results = []
        for seen, (name, measure_index, grain_indexes, aggregate) in \
                zip(self._grain_values, self.grain_measures):
            grain_results.append(
                _finalize_grain_state(seen, aggregate))

        count_results = [len(identities)
                         for identities in self._count_identities]

        return row_results, grain_results, count_results, self.record_count


# ---------------------------------------------------------------------------
# NULL-aware scalar aggregation states
#
# A state is a small mutable structure:
#   sum/min/max : [value_or_None]
#   average     : [count, total]
# (Keeping a one-element list lets the helpers update it in place.)
# ---------------------------------------------------------------------------

def _new_row_state(aggregate):
    if aggregate == "average":
        return [0, 0]
    return [None]


def _update_row_state(state, aggregate, value):
    if value is None:
        # sum ignores NULL; min/max ignore NULL as well
        return

    if aggregate == "sum":
        if state[0] is None:
            state[0] = value
        else:
            state[0] = state[0] + value
    elif aggregate == "min":
        if state[0] is None or value < state[0]:
            state[0] = value
    elif aggregate == "max":
        if state[0] is None or value > state[0]:
            state[0] = value
    elif aggregate == "average":
        state[0] += 1
        state[1] += value
    else:
        raise ArgumentError("Unknown aggregation '%s'" % aggregate)


def _finalize_row_state(state, aggregate):
    if aggregate == "average":
        if state[0] == 0:
            return None
        return state[1] / state[0]
    return state[0]


def _finalize_grain_state(seen, aggregate):
    """Reduces one `{identity: value}` mapping into the aggregate result.
    NULL values and identities without any non-NULL value are ignored; an
    all-NULL grain yields NULL."""

    values = [value for value in seen.values() if value is not None]

    if aggregate == "sum":
        if not values:
            return None
        total = values[0]
        for value in values[1:]:
            total = total + value
        return total
    elif aggregate == "min":
        return min(values) if values else None
    elif aggregate == "max":
        return max(values) if values else None
    elif aggregate == "average":
        if not values:
            return None
        return sum(values) / len(values)
    elif aggregate == "count":
        # Count of surviving identities at the grain, NULL-inclusive
        return len(seen)
    else:
        raise ArgumentError("Unknown aggregation '%s'" % aggregate)
