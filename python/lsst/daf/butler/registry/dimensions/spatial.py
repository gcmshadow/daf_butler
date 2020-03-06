# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = ["SpatialDimensionRecordStorage"]

from typing import Optional

import sqlalchemy

from ...core import DimensionElement, DimensionRecord, Timespan, ddl
from ...core.utils import NamedKeyDict, NamedValueSet
from ...core.dimensions.schema import makeElementTableSpec, REGION_FIELD_SPEC, addDimensionForeignKey
from ..interfaces import Database, DimensionRecordStorage, StaticTablesContext
from ..queries import QueryBuilder
from .table import TableDimensionRecordStorage


_OVERLAP_TABLE_NAME_PATTERN = "{0}_{1}_overlap"


def _makeOverlapTableSpec(a: DimensionElement, b: DimensionElement) -> ddl.TableSpec:
    """Create a specification for a table that represents a many-to-many
    relationship between two `DimensionElement` tables.

    Parameters
    ----------
    a : `DimensionElement`
        First element in the relationship.
    b : `DimensionElement`
        Second element in the relationship.

    Returns
    -------
    spec : `TableSpec`
        Database-agnostic specification for a table.
    """
    tableSpec = ddl.TableSpec(
        fields=NamedValueSet(),
        unique=set(),
        foreignKeys=[],
    )
    for dimension in a.graph.required:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
    for dimension in b.graph.required:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
    return tableSpec


class SpatialDimensionRecordStorage(TableDimensionRecordStorage):
    """A record storage implementation for spatial dimension elements that uses
    a regular database table.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    element : `DimensionElement`
        The element whose records this storage will manage.
    table : `sqlalchemy.schema.Table`
        The logical table for the element.
    commonSkyPixOverlapTable : `sqlalchemy.schema.Table`, optional
        The logical table for the overlap table with the dimension universe's
        common skypix dimension.
    """
    def __init__(self, db: Database, element: DimensionElement, *, table: sqlalchemy.schema.Table,
                 commonSkyPixOverlapTable: sqlalchemy.schema.Table):
        super().__init__(db, element, table=table)
        self._commonSkyPixOverlapTable = commonSkyPixOverlapTable
        assert element.spatial

    @classmethod
    def initialize(cls, db: Database, element: DimensionElement, *,
                   context: Optional[StaticTablesContext] = None) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorage.
        if context is not None:
            method = context.addTable
        else:
            method = db.ensureTableExists
        return cls(
            db,
            element,
            table=method(element.name, makeElementTableSpec(element)),
            commonSkyPixOverlapTable=method(
                _OVERLAP_TABLE_NAME_PATTERN.format(element.name, element.universe.commonSkyPix.name),
                _makeOverlapTableSpec(element, element.universe.commonSkyPix)
            )
        )

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ):
        # Docstring inherited from DimensionRecordStorage.
        if regions is not None:
            dimensions = NamedValueSet(self.element.graph.required)
            dimensions.add(self.element.universe.universe.commonSkyPix)
            builder.joinTable(self._commonSkyPixOverlapTable, dimensions)
            regions[self.element] = self._table.columns[REGION_FIELD_SPEC.name]
        super().join(builder, regions=None, timespans=timespans)

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        commonSkyPixRows = []
        commonSkyPix = self.element.universe.commonSkyPix
        for record in records:
            if record.region is None:
                # TODO: should we warn about this case?
                continue
            base = record.dataId.byName()
            for begin, end in commonSkyPix.pixelization.envelope(record.region):
                for skypix in range(begin, end):
                    row = base.copy()
                    row[commonSkyPix.name] = skypix
                    commonSkyPixRows.append(row)
        with self._db.transaction():
            super().insert(*records)
            if commonSkyPixRows:
                self._db.insert(self._commonSkyPixOverlapTable, *commonSkyPixRows)