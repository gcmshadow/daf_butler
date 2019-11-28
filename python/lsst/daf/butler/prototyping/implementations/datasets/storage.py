from __future__ import annotations

__all__ = ["ByDimensionsRegistryLayerDatasetStorage"]

from abc import abstractmethod
from typing import (
    Dict,
    Iterator,
    Optional,
    Type,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle, DatasetUniqueness
from ....core.dimensions import DimensionGraph, DimensionUniverse

from ...iterables import DatasetIterable
from ...interfaces import (
    Database,
    RegistryLayerCollectionStorage,
    RegistryLayerDatasetRecords,
    RegistryLayerDatasetStorage,
)
from .base import StaticDatasetTablesTuple, ByDimensionsRegistryLayerDatasetRecords


class ByDimensionsRegistryLayerDatasetStorage(RegistryLayerDatasetStorage):

    def __init__(self, *, db: Database, collections: RegistryLayerCollectionStorage,
                 RecordClasses: Dict[DatasetUniqueness, Type[ByDimensionsRegistryLayerDatasetRecords]]):
        self._db = db
        self._collections = collections
        self._static = StaticDatasetTablesTuple(db)
        self._RecordClasses = RecordClasses
        self._byName = {}
        self._byId = {}
        self.refresh()

    @classmethod
    @abstractmethod
    def loadTypes(cls, db: Database, collections: RegistryLayerCollectionStorage,
                  *, universe: DimensionUniverse) -> RegistryLayerDatasetStorage:
        # TODO: ideally we'd load RecordClasses from configuration, to make
        # this configurable all the way down.  Maybe attach a config to
        # Database?
        pass

    def refreshTypes(self, *, universe: DimensionUniverse):
        byName = {}
        byId = {}
        c = self._static.dataset_type.columns
        for row in self._layer.db.execute(self._static.dataset_type.select()).fetchall():
            name = row[c.name]
            dimensions = DimensionGraph.decode(row[c.dimensions_encoded], universe=universe)
            uniqueness = DatasetUniqueness(row[c.uniqueness])
            datasetType = DatasetType(name, dimensions, row[c.storage_class], uniqueness=uniqueness)
            records = self._RecordClasses[uniqueness].load(db=self._db, datasetType=datasetType,
                                                           static=self._static,
                                                           collections=self._collections,
                                                           id=row[c.id])
            byName[name] = records
            byId[records.id] = records
        self._byName = byName
        self._byId = byId

    def getType(self, datasetType: DatasetType) -> Optional[RegistryLayerDatasetRecords]:
        return self._records._byName(datasetType.name)

    def registerType(self, datasetType: DatasetType) -> RegistryLayerDatasetRecords:
        records = self._records.get(datasetType)
        if records is None:
            records = self._RecordClasses[datasetType.uniqueness].register(db=self._db,
                                                                           datasetType=datasetType,
                                                                           static=self._static,
                                                                           collection=self._collections)
            self._byName[datasetType.name] = records
            self._byId[records.id] = records
        return records

    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        return self._static.dataset_type

    def iterTypes(self) -> Iterator[RegistryLayerDatasetRecords]:
        yield from self._records.values()

    def getHandle(self, id: int, origin: int) -> Optional[ResolvedDatasetHandle]:
        sql = self._static.dataset.select().where(
            sqlalchemy.sql.and_(self._static.dataset.columns.id == id,
                                self._static.dataset.columns.origin == origin)
        )
        row = self._db.connection.execute(sql).fetchone()
        if row is None:
            return None
        recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
        if recordsForType is None:
            self.refresh()
            recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
            assert recordsForType is not None, "Should be guaranteed by foreign key constraints."
        return ResolvedDatasetHandle(
            recordsForType.datasetType,
            dataId=recordsForType.getDataId(id=id, origin=origin),
            id=id, origin=origin,
            run=self._collections.get(row[self._static.dataset.columns.run_id]).name
        )

    def insertLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                        ephemeral: bool = False):
        if ephemeral:
            raise NotImplementedError("Ephemeral datasets are not yet supported.")
        self.db.insert(
            self._static.dataset_location,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin, "datastore_name": datastoreName}
              for dataset in datasets]
        )

    def fetchLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        table = self._static.dataset_location
        sql = sqlalchemy.sql.select(
            [table.columns.datastore_name]
        ).select_from(table).where(
            sqlalchemy.sql.and_(
                table.columns.dataset_id == dataset.id,
                table.columns.origin == dataset.origin
            )
        )
        for row in self.db.connection.execute(sql, {"dataset_id": dataset.id,
                                                    "dataset_origin": dataset.origin}):
            yield row[table.columns.datastore_name]

    def deleteLocations(self, datastoreName: str, datasets: DatasetIterable):
        table = self._static.dataset_location
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                table.columns.datastore_name == datastoreName,
                table.columns.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                table.columns.origin == sqlalchemy.sql.bindparam("dataset_origin"),
            )
        )
        self.db.connection.execute(
            sql,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin} for dataset in datasets]
        )