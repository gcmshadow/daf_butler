from __future__ import annotations

__all__ = ["StaticTablesTuple", "CollectionType", "DatasetUniqueness", "STATIC_TABLES_SPEC"]

import enum
import hashlib
from namedtuple import namedtuple

import sqlalchemy

from ..core.dimensions import DimensionGraph
from ..core.dimensions.schema import TIMESPAN_FIELD_SPECS, addDimensionForeignKey
from ..core.schema import TableSpec, FieldSpec, ForeignKeySpec, Base64Bytes
from ..core.utils import NamedValueSet


StaticTablesTuple = namedtuple(
    "StaticTablesTuple",
    [
        "collection",
        "dataset_composition",
        "dataset_location",
        "dataset_type_dimension",
        "dataset_type",
        "dataset",
        "layer_meta_dimension",
        "layer_meta_opaque",
        "layer_meta_quantum",
        "quantum",
        "run",
    ]
)


class CollectionType(enum.IntEnum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


class DatasetUniqueness(enum.IntEnum):
    STANDARD = 1
    NONSINGULAR = 2
    GLOBAL = 3


STATIC_TABLES_SPEC = StaticTablesTuple(
    collection=TableSpec(
        fields=NamedValueSet([
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ]),
        unique={("name",)},
    ),
    dataset_composition=TableSpec(
        fields=NamedValueSet([
            FieldSpec("parent_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("parent_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_name", dtype=sqlalchemy.String, length=32),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset", source=("parent_dataset_id", "parent_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("component_dataset_id", "component_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
        ]
    ),
    dataset_location=TableSpec(
        fields=NamedValueSet([
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("datastore_name", dtype=sqlalchemy.String, length=256, primaryKey=True),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    ),
    dataset_type_dimension=TableSpec(
        fields=NamedValueSet([
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("dimension_name", dtype=sqlalchemy.String, length=32, primaryKey=True),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name"),
                           onDelete="CASCADE"),
        ]
    ),
    dataset_type=TableSpec(
        fields=NamedValueSet([
            FieldSpec("name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("storage_class", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("uniqueness", dtype=sqlalchemy.SmallInteger, nullable=False),
        ]),
    ),
    dataset=TableSpec(
        fields=NamedValueSet([
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128),
            FieldSpec("dataset_ref_hash", dtype=Base64Bytes, nbytes=32),
            FieldSpec("run_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name")),
            ForeignKeySpec("run", source=("run_id", "origin"), target=("collection_id", "origin"),
                           onDelete="CASCADE"),
        ]
    ),
    layer_meta_dimension=TableSpec(
        fields=NamedValueSet([
            FieldSpec("element_name", dtype=sqlalchemy.String, length=64, primaryKey=True),
        ]),
    ),
    layer_meta_opaque=TableSpec(
        fields=NamedValueSet([
            FieldSpec("table_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
        ]),
    ),
    layer_meta_quantum=TableSpec(
        fields=NamedValueSet([
            FieldSpec("dimensions_hash", dtype=sqlalchemy.String, length=32, primaryKey=True),
            FieldSpec("dimension_name", dtype=Base64Bytes, nbytes=32, primaryKey=True),
        ]),
    ),
    quantum=TableSpec(
        fields=NamedValueSet([
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("task", dtype=sqlalchemy.String, length=128),
            FieldSpec("run_collection_id", dtype=sqlalchemy.BigInteger, nullable=False),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=64),
            FieldSpec("config_id", dtype=sqlalchemy.BigInteger),
            FieldSpec("environment_id", dtype=sqlalchemy.BigInteger),
        ]),
        foreignKeys=[
            ForeignKeySpec("run", source=("run_collection_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("config_id", "origin"), target=("id", "origin"),
                           onDelete="SET NULL"),
            ForeignKeySpec("dataset", source=("environment_id", "origin"), target=("id", "origin"),
                           onDelete="SET NULL"),
        ],
    ),
    run=TableSpec(
        fields=NamedValueSet([
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=64),
        ]),
        unique={("name",)},
        foreignKeys=[
            ForeignKeySpec("collection", source=("collection_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ],
    ),
)


def hashQuantumDimensions(dimensions: DimensionGraph) -> bytes:
    message = hashlib.blake2b(digest_size=16)
    dimensions.fingerprint(message.update)
    return message.digest().hex()


QUANTUM_TABLE_NAME_FORMAT = "quantum_{}"


def makeQuantumTableSpec(dimensions: DimensionGraph) -> TableSpec:
    spec = TableSpec(
        fields=NamedValueSet([
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ]),
        foreignKeys=[
            ForeignKeySpec("quantum", source=("quantum_id", "origin"), target=("id", "origin")),
        ]
    )
    for dimension in dimensions:
        addDimensionForeignKey(spec, dimension, primaryKey=False,
                               nullable=(dimension in dimensions.required))
    return spec