# -*- coding: utf-8 -*-
from .objects import MongoDBCollection

from ...metadata import *
from ...errors import *
from ...prototypes import *
from ...objects import *
from ...grains import get_measure_grains

def prepare_mongo_key(key):
    key = prepare_key(key)
    return {name:1 for name in key}


#############################################################################
# Metadata Operations

@field_filter.register("mongo")
def _(ctx, obj, keep=None, drop=None, rename=None, filter=None):

    if rename:
        raise NotImplementedError("Renaming of MongoDB fields is not "
                                    "implemented")

    if filter:
        if keep or drop or rename:
            raise OperationError("Either filter or keep, drop, rename should "
                                 "be used")
        else:
            field_filter = filter
    else:
        grain_fields = []
        for declaration in get_measure_grains(obj).declarations():
            for name in declaration.grain:
                if name not in grain_fields:
                    grain_fields.append(name)
        field_filter = FieldFilter(keep=keep, drop=drop, rename=rename,
                                   grain_fields=grain_fields)

    new_fields = field_filter.filter(obj.fields)

    # No need to actually do anything just pretend that we have new fields.

    result = obj.clone(fields=new_fields)
    result.measure_grains = get_measure_grains(obj)
    return result

#############################################################################
# Row Operations


@distinct.register("mongo")
def _(ctx, obj, key=None):

    if not key:
        key = obj.fields.names()

    key = prepare_mongo_key(key)

    new_fields = obj.fields.fields(key)
    cursor = obj.collection.group(key, {}, {}, "function(obj, prev){}")
    return IterableRecordsDataSource(cursor, new_fields)

