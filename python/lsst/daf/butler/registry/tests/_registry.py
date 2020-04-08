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

__all__ = ["RegistryTests"]

from abc import ABC, abstractmethod
import os

import astropy.time
import sqlalchemy

from ...core import (
    DataCoordinate,
    DatasetType,
    DimensionGraph,
    StorageClass,
    ddl,
    YamlRepoImportBackend
)
from .._registry import Registry, CollectionType, ConflictingDefinitionError, OrphanedRecordError
from ..wildcards import DatasetTypeRestriction


class RegistryTests(ABC):
    """Generic tests for the `Registry` class that can be subclassed to
    generate tests for different configurations.
    """

    @classmethod
    @abstractmethod
    def getDataDir(cls) -> str:
        """Return the root directory containing test data YAML files.
        """
        raise NotImplementedError()

    @abstractmethod
    def makeRegistry(self) -> Registry:
        """Return the Registry instance to be tested.
        """
        raise NotImplementedError()

    def loadData(self, registry: Registry, filename: str):
        """Load registry test data from ``getDataDir/<filename>``,
        which should be a YAML import/export file.
        """
        with open(os.path.join(self.getDataDir(), filename), 'r') as stream:
            backend = YamlRepoImportBackend(stream, registry)
        backend.register()
        backend.load(datastore=None)

    def assertRowCount(self, registry: Registry, table: str, count: int):
        """Check the number of rows in table.
        """
        # TODO: all tests that rely on this method should be rewritten, as it
        # needs to depend on Registry implementation details to have any chance
        # of working.
        sql = sqlalchemy.sql.select(
            [sqlalchemy.sql.func.count()]
        ).select_from(
            getattr(registry._tables, table)
        )
        self.assertEqual(registry._db.query(sql).scalar(), count)

    def testOpaque(self):
        """Tests for `Registry.registerOpaqueTable`,
        `Registry.insertOpaqueData`, `Registry.fetchOpaqueData`, and
        `Registry.deleteOpaqueData`.
        """
        registry = self.makeRegistry()
        table = "opaque_table_for_testing"
        registry.registerOpaqueTable(
            table,
            spec=ddl.TableSpec(
                fields=[
                    ddl.FieldSpec("id", dtype=sqlalchemy.BigInteger, primaryKey=True),
                    ddl.FieldSpec("name", dtype=sqlalchemy.String, length=16, nullable=False),
                    ddl.FieldSpec("count", dtype=sqlalchemy.SmallInteger, nullable=True),
                ],
            )
        )
        rows = [
            {"id": 1, "name": "one", "count": None},
            {"id": 2, "name": "two", "count": 5},
            {"id": 3, "name": "three", "count": 6},
        ]
        registry.insertOpaqueData(table, *rows)
        self.assertCountEqual(rows, list(registry.fetchOpaqueData(table)))
        self.assertEqual(rows[0:1], list(registry.fetchOpaqueData(table, id=1)))
        self.assertEqual(rows[1:2], list(registry.fetchOpaqueData(table, name="two")))
        self.assertEqual([], list(registry.fetchOpaqueData(table, id=1, name="two")))
        registry.deleteOpaqueData(table, id=3)
        self.assertCountEqual(rows[:2], list(registry.fetchOpaqueData(table)))
        registry.deleteOpaqueData(table)
        self.assertEqual([], list(registry.fetchOpaqueData(table)))

    def testDatasetType(self):
        """Tests for `Registry.registerDatasetType` and
        `Registry.getDatasetType`.
        """
        registry = self.makeRegistry()
        # Check valid insert
        datasetTypeName = "test"
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = registry.dimensions.extract(("instrument", "visit"))
        differentDimensions = registry.dimensions.extract(("instrument", "patch"))
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        # Inserting for the first time should return True
        self.assertTrue(registry.registerDatasetType(inDatasetType))
        outDatasetType1 = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType1, inDatasetType)

        # Re-inserting should work
        self.assertFalse(registry.registerDatasetType(inDatasetType))
        # Except when they are not identical
        with self.assertRaises(ConflictingDefinitionError):
            nonIdenticalDatasetType = DatasetType(datasetTypeName, differentDimensions, storageClass)
            registry.registerDatasetType(nonIdenticalDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = StorageClass("testDatasetType2")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = registry.dimensions.extract(("instrument", "visit"))
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType2 = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType2, inDatasetType)

        allTypes = set(registry.queryDatasetTypes())
        self.assertEqual(allTypes, {outDatasetType1, outDatasetType2})

    def testDimensions(self):
        """Tests for `Registry.insertDimensionData` and
        `Registry.expandDataId`.
        """
        registry = self.makeRegistry()
        dimensionName = "instrument"
        dimension = registry.dimensions[dimensionName]
        dimensionValue = {"name": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2,
                          "class_name": "lsst.obs.base.Instrument"}
        registry.insertDimensionData(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            registry.insertDimensionData(dimensionName, dimensionValue)
        # expandDataId should retrieve the record we just inserted
        self.assertEqual(
            registry.expandDataId(
                instrument="DummyCam",
                graph=dimension.graph
            ).records[dimensionName].toDict(),
            dimensionValue
        )
        # expandDataId should raise if there is no record with the given ID.
        with self.assertRaises(LookupError):
            registry.expandDataId({"instrument": "Unknown"}, graph=dimension.graph)
        # abstract_filter doesn't have a table; insert should fail.
        with self.assertRaises(TypeError):
            registry.insertDimensionData("abstract_filter", {"abstract_filter": "i"})
        dimensionName2 = "physical_filter"
        dimension2 = registry.dimensions[dimensionName2]
        dimensionValue2 = {"name": "DummyCam_i", "abstract_filter": "i"}
        # Missing required dependency ("instrument") should fail
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            registry.insertDimensionData(dimensionName2, dimensionValue2)
        # Adding required dependency should fix the failure
        dimensionValue2["instrument"] = "DummyCam"
        registry.insertDimensionData(dimensionName2, dimensionValue2)
        # expandDataId should retrieve the record we just inserted.
        self.assertEqual(
            registry.expandDataId(
                instrument="DummyCam", physical_filter="DummyCam_i",
                graph=dimension2.graph
            ).records[dimensionName2].toDict(),
            dimensionValue2
        )

    def testDataset(self):
        """Basic tests for `Registry.insertDatasets`, `Registry.getDataset`,
        and `Registry.removeDataset`.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        run = "test"
        registry.registerRun(run)
        datasetType = registry.getDatasetType("permabias")
        dataId = {"instrument": "Cam1", "detector": 2}
        ref, = registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        outRef = registry.getDataset(ref.id)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref, outRef)
        with self.assertRaises(ConflictingDefinitionError):
            registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        registry.removeDataset(ref)
        self.assertIsNone(registry.findDataset(datasetType, dataId, collections=[run]))

    def testComponents(self):
        """Tests for `Registry.attachComponent` and other dataset operations
        on composite datasets.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        run = "test"
        registry.registerRun(run)
        parentDatasetType = registry.getDatasetType("permabias")
        childDatasetType1 = registry.getDatasetType("permabias.image")
        childDatasetType2 = registry.getDatasetType("permabias.mask")
        dataId = {"instrument": "Cam1", "detector": 2}
        parent, = registry.insertDatasets(parentDatasetType, dataIds=[dataId], run=run)
        children = {"image": registry.insertDatasets(childDatasetType1, dataIds=[dataId], run=run)[0],
                    "mask": registry.insertDatasets(childDatasetType2, dataIds=[dataId], run=run)[0]}
        for name, child in children.items():
            registry.attachComponent(name, parent, child)
        self.assertEqual(parent.components, children)
        outParent = registry.getDataset(parent.id)
        self.assertEqual(outParent.components, children)
        # Remove the parent; this should remove all children.
        registry.removeDataset(parent)
        self.assertIsNone(registry.findDataset(parentDatasetType, dataId, collections=[run]))
        self.assertIsNone(registry.findDataset(childDatasetType1, dataId, collections=[run]))
        self.assertIsNone(registry.findDataset(childDatasetType2, dataId, collections=[run]))

    def testFindDataset(self):
        """Tests for `Registry.findDataset`.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        run = "test"
        datasetType = registry.getDatasetType("permabias")
        dataId = {"instrument": "Cam1", "detector": 4}
        registry.registerRun(run)
        inputRef, = registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        outputRef = registry.findDataset(datasetType, dataId, collections=[run])
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(LookupError):
            dataId = {"instrument": "Cam1"}  # no detector
            registry.findDataset(datasetType, dataId, collections=run)
        # Check that different dataIds match to different datasets
        dataId1 = {"instrument": "Cam1", "detector": 1}
        inputRef1, = registry.insertDatasets(datasetType, dataIds=[dataId1], run=run)
        dataId2 = {"instrument": "Cam1", "detector": 2}
        inputRef2, = registry.insertDatasets(datasetType, dataIds=[dataId2], run=run)
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=run), inputRef1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=run), inputRef2)
        self.assertNotEqual(registry.findDataset(datasetType, dataId1, collections=run), inputRef2)
        self.assertNotEqual(registry.findDataset(datasetType, dataId2, collections=run), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"instrument": "Cam1", "detector": 3}
        self.assertIsNone(registry.findDataset(datasetType, nonExistingDataId, collections=run))

    def testCollections(self):
        """Tests for registry methods that manage collections.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        run1 = "imported_g"
        run2 = "imported_r"
        datasetType = "permabias"
        # Find some datasets via their run's collection.
        dataId1 = {"instrument": "Cam1", "detector": 1}
        ref1 = registry.findDataset(datasetType, dataId1, collections=run1)
        self.assertIsNotNone(ref1)
        dataId2 = {"instrument": "Cam1", "detector": 2}
        ref2 = registry.findDataset(datasetType, dataId2, collections=run1)
        self.assertIsNotNone(ref2)
        # Associate those into a new collection,then look for them there.
        tag1 = "tag1"
        registry.registerCollection(tag1, type=CollectionType.TAGGED)
        registry.associate(tag1, [ref1, ref2])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Disassociate one and verify that we can't it there anymore...
        registry.disassociate(tag1, [ref1])
        self.assertIsNone(registry.findDataset(datasetType, dataId1, collections=tag1))
        # ...but we can still find ref2 in tag1, and ref1 in the run.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=run1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        collections = set(registry.queryCollections())
        self.assertEqual(collections, {run1, run2, tag1})
        # Associate both refs into tag1 again; ref2 is already there, but that
        # should be a harmless no-op.
        registry.associate(tag1, [ref1, ref2])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Get a different dataset (from a different run) that has the same
        # dataset type and data ID as ref2.
        ref2b = registry.findDataset(datasetType, dataId2, collections=run2)
        self.assertNotEqual(ref2, ref2b)
        # Attempting to associate that into tag1 should be an error.
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(tag1, [ref2b])
        # That error shouldn't have messed up what we had before.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Attempt to associate the conflicting dataset again, this time with
        # a dataset that isn't in the collection and won't cause a conflict.
        # Should also fail without modifying anything.
        dataId3 = {"instrument": "Cam1", "detector": 3}
        ref3 = registry.findDataset(datasetType, dataId3, collections=run1)
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(tag1, [ref3, ref2b])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        self.assertIsNone(registry.findDataset(datasetType, dataId3, collections=tag1))
        # Register a chained collection that searches:
        # 1. 'tag1'
        # 2. 'run1', but only for the permaflat dataset
        # 3. 'run2'
        chain1 = "chain1"
        registry.registerCollection(chain1, type=CollectionType.CHAINED)
        self.assertIs(registry.getCollectionType(chain1), CollectionType.CHAINED)
        # Chained collection exists, but has no collections in it.
        self.assertFalse(registry.getCollectionChain(chain1))
        # If we query for all collections, we should get the chained collection
        # only if we don't ask to flatten it (i.e. yield only its children).
        self.assertEqual(set(registry.queryCollections(flattenChains=False)), {tag1, run1, run2, chain1})
        self.assertEqual(set(registry.queryCollections(flattenChains=True)), {tag1, run1, run2})
        # Attempt to set its child collections to something circular; that
        # should fail.
        with self.assertRaises(ValueError):
            registry.setCollectionChain(chain1, [tag1, chain1])
        # Add the child collections.
        registry.setCollectionChain(chain1, [tag1, (run1, "permaflat"), run2])
        self.assertEqual(
            list(registry.getCollectionChain(chain1)),
            [(tag1, DatasetTypeRestriction.any),
             (run1, DatasetTypeRestriction.fromExpression("permaflat")),
             (run2, DatasetTypeRestriction.any)]
        )
        # Searching for dataId1 or dataId2 in the chain should return ref1 and
        # ref2, because both are in tag1.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=chain1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=chain1), ref2)
        # Now disassociate ref2 from tag1.  The search (for permabias) with
        # dataId2 in chain1 should then:
        # 1. not find it in tag1
        # 2. not look in tag2, because it's restricted to permaflat here
        # 3. find a different dataset in run2
        registry.disassociate(tag1, [ref2])
        ref2b = registry.findDataset(datasetType, dataId2, collections=chain1)
        self.assertNotEqual(ref2b, ref2)
        self.assertEqual(ref2b, registry.findDataset(datasetType, dataId2, collections=run2))
        # Look in the chain for a permaflat that is in run1; should get the
        # same ref as if we'd searched run1 directly.
        dataId3 = {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}
        self.assertEqual(registry.findDataset("permaflat", dataId3, collections=chain1),
                         registry.findDataset("permaflat", dataId3, collections=run1),)
        # Define a new chain so we can test recursive chains.
        chain2 = "chain2"
        registry.registerCollection(chain2, type=CollectionType.CHAINED)
        registry.setCollectionChain(chain2, [(run2, "permabias"), chain1])
        # Search for permabias with dataId1 should find it via tag1 in chain2,
        # recursing, because is not in run1.
        self.assertIsNone(registry.findDataset(datasetType, dataId1, collections=run2))
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=chain2), ref1)
        # Search for permabias with dataId2 should find it in run2 (ref2b).
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=chain2), ref2b)
        # Search for a permaflat that is in run2.  That should not be found
        # at the front of chain2, because of the restriction to permabias
        # on run2 there, but it should be found in at the end of chain1.
        dataId4 = {"instrument": "Cam1", "detector": 3, "physical_filter": "Cam1-R2"}
        ref4 = registry.findDataset("permaflat", dataId4, collections=run2)
        self.assertIsNotNone(ref4)
        self.assertEqual(ref4, registry.findDataset("permaflat", dataId4, collections=chain2))

    def testDatasetLocations(self):
        """Tests for `Registry.insertDatasetLocations`,
        `Registry.getDatasetLocations`, and `Registry.removeDatasetLocations`.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        run = "imported_g"
        ref = registry.findDataset("permabias", dataId={"instrument": "Cam1", "detector": 1}, collections=run)
        ref2 = registry.findDataset("permaflat",
                                    dataId={"instrument": "Cam1", "detector": 3, "physical_filter": "Cam1-G"},
                                    collections=run)
        datastoreName = "dummystore"
        datastoreName2 = "dummystore2"
        # Test adding information about a new dataset
        registry.insertDatasetLocations(datastoreName, [ref])
        addresses = registry.getDatasetLocations(ref)
        self.assertIn(datastoreName, addresses)
        self.assertEqual(len(addresses), 1)
        registry.insertDatasetLocations(datastoreName2, [ref, ref2])
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 2)
        self.assertIn(datastoreName, addresses)
        self.assertIn(datastoreName2, addresses)
        registry.removeDatasetLocation(datastoreName, ref)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 1)
        self.assertNotIn(datastoreName, addresses)
        self.assertIn(datastoreName2, addresses)
        with self.assertRaises(OrphanedRecordError):
            registry.removeDataset(ref)
        registry.removeDatasetLocation(datastoreName2, ref)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 0)
        self.assertNotIn(datastoreName2, addresses)
        registry.removeDataset(ref)  # should not raise
        addresses = registry.getDatasetLocations(ref2)
        self.assertEqual(len(addresses), 1)
        self.assertIn(datastoreName2, addresses)

    def testBasicTransaction(self):
        """Test that all operations within a single transaction block are
        rolled back if an exception propagates out of the block.
        """
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = registry.dimensions.extract(("instrument",))
        dataId = {"instrument": "DummyCam"}
        datasetTypeA = DatasetType(name="A",
                                   dimensions=dimensions,
                                   storageClass=storageClass)
        datasetTypeB = DatasetType(name="B",
                                   dimensions=dimensions,
                                   storageClass=storageClass)
        datasetTypeC = DatasetType(name="C",
                                   dimensions=dimensions,
                                   storageClass=storageClass)
        run = "test"
        registry.registerRun(run)
        refId = None
        with registry.transaction():
            registry.registerDatasetType(datasetTypeA)
        with self.assertRaises(ValueError):
            with registry.transaction():
                registry.registerDatasetType(datasetTypeB)
                registry.registerDatasetType(datasetTypeC)
                registry.insertDimensionData("instrument", {"instrument": "DummyCam"})
                ref, = registry.insertDatasets(datasetTypeA, dataIds=[dataId], run=run)
                refId = ref.id
                raise ValueError("Oops, something went wrong")
        # A should exist
        self.assertEqual(registry.getDatasetType("A"), datasetTypeA)
        # But B and C should both not exist
        with self.assertRaises(KeyError):
            registry.getDatasetType("B")
        with self.assertRaises(KeyError):
            registry.getDatasetType("C")
        # And neither should the dataset
        self.assertIsNotNone(refId)
        self.assertIsNone(registry.getDataset(refId))
        # Or the Dimension entries
        with self.assertRaises(LookupError):
            registry.expandDataId({"instrument": "DummyCam"})

    def testNestedTransaction(self):
        """Test that operations within a transaction block are not rolled back
        if an exception propagates out of an inner transaction block and is
        then caught.
        """
        registry = self.makeRegistry()
        dimension = registry.dimensions["instrument"]
        dataId1 = {"instrument": "DummyCam"}
        dataId2 = {"instrument": "DummyCam2"}
        checkpointReached = False
        with registry.transaction():
            # This should be added and (ultimately) committed.
            registry.insertDimensionData(dimension, dataId1)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                with registry.transaction():
                    # This does not conflict, and should succeed (but not
                    # be committed).
                    registry.insertDimensionData(dimension, dataId2)
                    checkpointReached = True
                    # This should conflict and raise, triggerring a rollback
                    # of the previous insertion within the same transaction
                    # context, but not the original insertion in the outer
                    # block.
                    registry.insertDimensionData(dimension, dataId1)
        self.assertTrue(checkpointReached)
        self.assertIsNotNone(registry.expandDataId(dataId1, graph=dimension.graph))
        with self.assertRaises(LookupError):
            registry.expandDataId(dataId2, graph=dimension.graph)

    def testInstrumentDimensions(self):
        """Test queries involving only instrument dimensions, with no joins to
        skymap."""
        registry = self.makeRegistry()

        # need a bunch of dimensions and datasets for test
        registry.insertDimensionData(
            "instrument",
            dict(name="DummyCam", visit_max=25, exposure_max=300, detector_max=6)
        )
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", abstract_filter="r"),
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "detector",
            *[dict(instrument="DummyCam", id=i, full_name=str(i)) for i in range(1, 6)]
        )
        registry.insertDimensionData(
            "visit",
            dict(instrument="DummyCam", id=10, name="ten", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=11, name="eleven", physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=20, name="twelve", physical_filter="dummy_r"),
        )
        registry.insertDimensionData(
            "exposure",
            dict(instrument="DummyCam", id=100, name="100", visit=10, physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=101, name="101", visit=10, physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=110, name="110", visit=11, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=111, name="111", visit=11, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=200, name="200", visit=20, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=201, name="201", visit=20, physical_filter="dummy_r"),
        )
        # dataset types
        run1 = "test1_r"
        run2 = "test2_r"
        tagged2 = "test2_t"
        registry.registerRun(run1)
        registry.registerRun(run2)
        registry.registerCollection(tagged2)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW",
                              dimensions=registry.dimensions.extract(("instrument", "exposure", "detector")),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(name="CALEXP",
                                 dimensions=registry.dimensions.extract(("instrument", "visit", "detector")),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        # add pre-existing datasets
        for exposure in (100, 101, 110, 111):
            for detector in (1, 2, 3):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                ref, = registry.insertDatasets(rawType, dataIds=[dataId], run=run1)
                # exposures 100 and 101 appear in both run1 and tagged2.
                # 100 has different datasets in the different collections
                # 101 has the same dataset in both collections.
                if exposure == 100:
                    ref, = registry.insertDatasets(rawType, dataIds=[dataId], run=run2)
                if exposure in (100, 101):
                    registry.associate(tagged2, [ref])
        # Add pre-existing datasets to tagged2.
        for exposure in (200, 201):
            for detector in (3, 4, 5):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                ref, = registry.insertDatasets(rawType, dataIds=[dataId], run=run2)
                registry.associate(tagged2, [ref])

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(rawType.dimensions.required | calexpType.dimensions.required)
        )
        # Test that single dim string works as well as list of str
        rows = list(registry.queryDimensions("visit", datasets=rawType, collections=run1, expand=True))
        rowsI = list(registry.queryDimensions(["visit"], datasets=rawType, collections=run1, expand=True))
        self.assertEqual(rows, rowsI)
        # with empty expression
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=run1, expand=True))
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
            packer1 = registry.dimensions.makePacker("visit_detector", dataId)
            packer2 = registry.dimensions.makePacker("exposure_detector", dataId)
            self.assertEqual(packer1.unpack(packer1.pack(dataId)),
                             DataCoordinate.standardize(dataId, graph=packer1.dimensions))
            self.assertEqual(packer2.unpack(packer2.pack(dataId)),
                             DataCoordinate.standardize(dataId, graph=packer2.dimensions))
            self.assertNotEqual(packer1.pack(dataId), packer2.pack(dataId))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # second collection
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=tagged2))
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=[run1, tagged2]))
        self.assertEqual(len(set(rows)), 6*3)   # 6 exposures times 3 detectors; set needed to de-dupe
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=run1,
                                             where="visit = 10"))
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # more limiting expression, using link names instead of Table.column
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=run1,
                                             where="visit = 10 and detector > 1"))
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (2, 3))

        # expression excludes everything
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=run1,
                                             where="visit > 1000"))
        self.assertEqual(len(rows), 0)

        # Selecting by physical_filter, this is not in the dimensions, but it
        # is a part of the full expression so it should work too.
        rows = list(registry.queryDimensions(dimensions, datasets=rawType, collections=run1,
                                             where="physical_filter = 'dummy_r'"))
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (11,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

    def testSkyMapDimensions(self):
        """Tests involving only skymap dimensions, no joins to instrument."""
        registry = self.makeRegistry()

        # need a bunch of dimensions and datasets for test, we want
        # "abstract_filter" in the test so also have to add physical_filter
        # dimensions
        registry.insertDimensionData(
            "instrument",
            dict(instrument="DummyCam")
        )
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", abstract_filter="r"),
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "skymap",
            dict(name="DummyMap", hash="sha!".encode("utf8"))
        )
        for tract in range(10):
            registry.insertDimensionData("tract", dict(skymap="DummyMap", id=tract))
            registry.insertDimensionData(
                "patch",
                *[dict(skymap="DummyMap", tract=tract, id=patch, cell_x=0, cell_y=0)
                  for patch in range(10)]
            )

        # dataset types
        run = "test"
        registry.registerRun(run)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        calexpType = DatasetType(name="deepCoadd_calexp",
                                 dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                         "abstract_filter")),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(name="deepCoadd_mergeDet",
                                dimensions=registry.dimensions.extract(("skymap", "tract", "patch")),
                                storageClass=storageClass)
        registry.registerDatasetType(mergeType)
        measType = DatasetType(name="deepCoadd_meas",
                               dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                       "abstract_filter")),
                               storageClass=storageClass)
        registry.registerDatasetType(measType)

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(calexpType.dimensions.required | mergeType.dimensions.required
                        | measType.dimensions.required)
        )

        # add pre-existing datasets
        for tract in (1, 3, 5):
            for patch in (2, 4, 6, 7):
                dataId = dict(skymap="DummyMap", tract=tract, patch=patch)
                registry.insertDatasets(mergeType, dataIds=[dataId], run=run)
                for aFilter in ("i", "r"):
                    dataId = dict(skymap="DummyMap", tract=tract, patch=patch, abstract_filter=aFilter)
                    registry.insertDatasets(calexpType, dataIds=[dataId], run=run)

        # with empty expression
        rows = list(registry.queryDimensions(dimensions,
                                             datasets=[calexpType, mergeType], collections=run))
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        rows = list(registry.queryDimensions(dimensions,
                                             datasets=[calexpType, mergeType], collections=run,
                                             where="tract IN (1, 5) AND patch IN (2, 7)"))
        self.assertEqual(len(rows), 2*2*2)   # 2 tracts x 2 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to single filter
        rows = list(registry.queryDimensions(dimensions,
                                             datasets=[calexpType, mergeType], collections=run,
                                             where="abstract_filter = 'i'"))
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i",))

        # expression excludes everything, specifying non-existing skymap is
        # not a fatal error, it's operator error
        rows = list(registry.queryDimensions(dimensions,
                                             datasets=[calexpType, mergeType], collections=run,
                                             where="skymap = 'Mars'"))
        self.assertEqual(len(rows), 0)

    def testSpatialMatch(self):
        """Test involving spatial match using join tables.

        Note that realistic test needs a reasonably-defined skypix and regions
        in registry tables which is hard to implement in this simple test.
        So we do not actually fill registry with any data and all queries will
        return empty result, but this is still useful for coverage of the code
        that generates query.
        """
        registry = self.makeRegistry()

        # dataset types
        collection = "test"
        registry.registerRun(name=collection)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)

        calexpType = DatasetType(name="CALEXP",
                                 dimensions=registry.dimensions.extract(("instrument", "visit", "detector")),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        coaddType = DatasetType(name="deepCoadd_calexp",
                                dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                        "abstract_filter")),
                                storageClass=storageClass)
        registry.registerDatasetType(coaddType)

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(calexpType.dimensions.required | coaddType.dimensions.required)
        )

        # without data this should run OK but return empty set
        rows = list(registry.queryDimensions(dimensions, datasets=calexpType, collections=collection))
        self.assertEqual(len(rows), 0)

    def testCalibrationLabelIndirection(self):
        """Test that we can look up datasets with calibration_label dimensions
        from a data ID with exposure dimensions.
        """

        def _dt(iso_string):
            return astropy.time.Time(iso_string, format="iso", scale="utc")

        registry = self.makeRegistry()

        flat = DatasetType(
            "flat",
            registry.dimensions.extract(
                ["instrument", "detector", "physical_filter", "calibration_label"]
            ),
            "ImageU"
        )
        registry.registerDatasetType(flat)
        registry.insertDimensionData("instrument", dict(name="DummyCam"))
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "detector",
            *[dict(instrument="DummyCam", id=i, full_name=str(i)) for i in (1, 2, 3, 4, 5)]
        )
        registry.insertDimensionData(
            "visit",
            dict(instrument="DummyCam", id=10, name="ten", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=11, name="eleven", physical_filter="dummy_i"),
        )
        registry.insertDimensionData(
            "exposure",
            dict(instrument="DummyCam", id=100, name="100", visit=10, physical_filter="dummy_i",
                 datetime_begin=_dt("2005-12-15 02:00:00"), datetime_end=_dt("2005-12-15 03:00:00")),
            dict(instrument="DummyCam", id=101, name="101", visit=11, physical_filter="dummy_i",
                 datetime_begin=_dt("2005-12-16 02:00:00"), datetime_end=_dt("2005-12-16 03:00:00")),
        )
        registry.insertDimensionData(
            "calibration_label",
            dict(instrument="DummyCam", name="first_night",
                 datetime_begin=_dt("2005-12-15 01:00:00"), datetime_end=_dt("2005-12-15 04:00:00")),
            dict(instrument="DummyCam", name="second_night",
                 datetime_begin=_dt("2005-12-16 01:00:00"), datetime_end=_dt("2005-12-16 04:00:00")),
            dict(instrument="DummyCam", name="both_nights",
                 datetime_begin=_dt("2005-12-15 01:00:00"), datetime_end=_dt("2005-12-16 04:00:00")),
        )
        # Different flats for different nights for detectors 1-3 in first
        # collection.
        run1 = "calibs1"
        registry.registerRun(run1)
        for detector in (1, 2, 3):
            registry.insertDatasets(flat, [dict(instrument="DummyCam", calibration_label="first_night",
                                                physical_filter="dummy_i", detector=detector)],
                                    run=run1)
            registry.insertDatasets(flat, [dict(instrument="DummyCam", calibration_label="second_night",
                                                physical_filter="dummy_i", detector=detector)],
                                    run=run1)
        # The same flat for both nights for detectors 3-5 (so detector 3 has
        # multiple valid flats) in second collection.
        run2 = "calib2"
        registry.registerRun(run2)
        for detector in (3, 4, 5):
            registry.insertDatasets(flat, [dict(instrument="DummyCam", calibration_label="both_nights",
                                                physical_filter="dummy_i", detector=detector)],
                                    run=run2)
        # Perform queries for individual exposure+detector combinations, which
        # should always return exactly one flat.
        for exposure in (100, 101):
            for detector in (1, 2, 3):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = list(registry.queryDatasets("flat", collections=[run1],
                                                       instrument="DummyCam",
                                                       exposure=exposure,
                                                       detector=detector))
                    self.assertEqual(len(rows), 1)
            for detector in (3, 4, 5):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[run2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.assertEqual(len(list(rows)), 1)
            for detector in (1, 2, 4, 5):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[run1, run2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.assertEqual(len(list(rows)), 1)
            for detector in (3,):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[run1, run2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.assertEqual(len(list(rows)), 2)

    def testAbstractFilterQuery(self):
        """Test that we can run a query that just lists the known
        abstract_filters.  This is tricky because abstract_filter is
        backed by a query against physical_filter.
        """
        registry = self.makeRegistry()
        registry.insertDimensionData("instrument", dict(name="DummyCam"))
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
            dict(instrument="DummyCam", name="dummy_i2", abstract_filter="i"),
            dict(instrument="DummyCam", name="dummy_r", abstract_filter="r"),
        )
        rows = list(registry.queryDimensions(["abstract_filter"]))
        self.assertCountEqual(
            rows,
            [DataCoordinate.standardize(abstract_filter="i", universe=registry.dimensions),
             DataCoordinate.standardize(abstract_filter="r", universe=registry.dimensions)]
        )
