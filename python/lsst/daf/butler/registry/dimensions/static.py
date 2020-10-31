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

from typing import Dict, List, Optional

import sqlalchemy

from ...core import (
    ddl,
    DimensionElement,
    DimensionGraph,
    DimensionUniverse,
    NamedKeyDict,
)
from ..interfaces import (
    Database,
    StaticTablesContext,
    DimensionRecordStorageManager,
    DimensionRecordStorage,
    GovernorDimensionRecordStorage,
    VersionTuple
)


# This has to be updated on every schema change
_VERSION = VersionTuple(5, 0, 0)


def _makeDimensionGraphTableSpec() -> ddl.TableSpec:
    """Create a specification for a table that holds `DimensionGraph`
    definitions.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for the table.
    """
    return ddl.TableSpec(
        fields=[
            ddl.FieldSpec(
                name="digest",
                dtype=sqlalchemy.String,
                length=DimensionGraph.DIGEST_SIZE,
                primaryKey=True,
            ),
            ddl.FieldSpec(
                name="dimension_name",
                dtype=sqlalchemy.String,
                length=64,
                primaryKey=True,
            ),
        ]
    )


class StaticDimensionRecordStorageManager(DimensionRecordStorageManager):
    """An implementation of `DimensionRecordStorageManager` for single-layer
    `Registry` and the base layers of multi-layer `Registry`.

    This manager creates `DimensionRecordStorage` instances for all elements
    in the `DimensionUniverse` in its own `initialize` method, as part of
    static table creation, so it never needs to manage any dynamic registry
    tables.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    records : `NamedKeyDict`
        Mapping from `DimensionElement` to `DimensionRecordStorage` for that
        element.
    dimensionGraphTable : `sqlalchemy.schema.Table`
        Tables that holds `DimensionGraph` definitions.
    universe : `DimensionUniverse`
        All known dimensions.
    """
    def __init__(
        self,
        db: Database, *,
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage],
        dimensionGraphTable: sqlalchemy.schema.Table,
        universe: DimensionUniverse,
    ):
        super().__init__(universe=universe)
        self._db = db
        self._records = records
        self._dimensionGraphTable = dimensionGraphTable
        self._dimensionGraphCache: Dict[str, DimensionGraph] = {}

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   universe: DimensionUniverse) -> DimensionRecordStorageManager:
        # Docstring inherited from DimensionRecordStorageManager.
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage] = NamedKeyDict()
        for element in universe.getStaticElements():
            records[element] = element.makeStorage(db, context=context)
        # Create table that stores DimensionGraph definitions.
        dimensionGraphTable = context.addTable("dimension_graph", _makeDimensionGraphTableSpec())
        return cls(db=db, records=records, universe=universe,
                   dimensionGraphTable=dimensionGraphTable)

    def refresh(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for dimension in self.universe.getGovernorDimensions():
            storage = self._records[dimension]
            assert isinstance(storage, GovernorDimensionRecordStorage)
            storage.refresh()

    def get(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._records.get(element)

    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorageManager.
        result = self._records.get(element)
        assert result, "All records instances should be created in initialize()."
        return result

    def saveDimensionGraph(self, graph: DimensionGraph) -> str:
        # Docstring inherited from DimensionRecordStorageManager.
        digest = graph.digest()
        if digest not in self._dimensionGraphCache:
            rows = [{"digest": digest, "dimension_name": name} for name in graph.required.names]
            self._db.ensure(self._dimensionGraphTable, *rows)
            self._dimensionGraphCache[digest] = graph
        return digest

    def loadDimensionGraph(self, digest: str) -> DimensionGraph:
        # Docstring inherited from DimensionRecordStorageManager.
        graph = self._dimensionGraphCache.get(digest)
        if graph is None:
            sql = sqlalchemy.sql.select(
                [self._dimensionGraphTable.columns.dimension_name.label("name")]
            ).select_from(
                self._dimensionGraphTable
            ).where(
                self._dimensionGraphTable.columns.digest == digest
            )
            names = [row["name"] for row in self._db.query(sql)]
            graph = DimensionGraph(universe=self.universe, names=names)
            if graph.digest() != digest:
                if not names:
                    raise LookupError(f"DimensionGraph with digest {digest} not found in database.")
                raise RuntimeError(
                    f"DimensionGraph loaded with digest={digest} has digest={graph.digest()}. "
                    "This is either a logic bug or an incompatible change to the dimension definitions "
                    "that requires a database migration."
                )
            self._dimensionGraphCache[digest] = graph
        return graph

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for storage in self._records.values():
            storage.clearCaches()
        self._dimensionGraphCache.clear()

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        tables: List[sqlalchemy.schema.Table] = []
        for recStorage in self._records.values():
            tables += recStorage.digestTables()
        return self._defaultSchemaDigest(tables, self._db.dialect)
