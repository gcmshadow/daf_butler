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

__all__ = ["RepoExportContext"]

from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)
from collections import defaultdict

from ..core import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    DatasetRef,
    DatasetType,
    Datastore,
    FileDataset,
)
from ..registry import Registry
from ._interfaces import RepoExportBackend


class RepoExportContext:
    """Public interface for exporting a subset of a data repository.

    Instances of this class are obtained by calling `Butler.export` as the
    value returned by that context manager::

        with butler.export(filename="export.yaml") as export:
            export.saveDataIds(...)
            export.saveDatasets(...)

    Parameters
    ----------
    registry : `Registry`
        Registry to export from.
    datastore : `Datastore`
        Datastore to export from.
    backend : `RepoExportBackend`
        Implementation class for a particular export file format.
    directory : `str`, optional
        Directory to pass to `Datastore.export`.
    transfer : `str`, optional
        Transfer mdoe to pass to `Datastore.export`.
    """

    def __init__(self, registry: Registry, datastore: Datastore, backend: RepoExportBackend, *,
                 directory: Optional[str] = None, transfer: Optional[str] = None):
        self._registry = registry
        self._datastore = datastore
        self._backend = backend
        self._directory = directory
        self._transfer = transfer
        self._records: Dict[DimensionElement, Dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
        self._dataset_ids = set()
        self._datasets: Dict[Tuple[DatasetType, str], List[FileDataset]] = defaultdict(list)

    def saveDimensionData(self, records: Iterable[DimensionRecord]) -> None:
        """Export the given dimension records associated with one or more data
        IDs.

        Parameters
        ----------
        records : `Iterable` [ `DimensionRecord` ]
            Records to export.
        """
        for record in records:
            self._records[record.definition].setdefault(record.dataId, record)

    def saveDataIds(self, dataIds: Iterable[DataCoordinate], *,
                    elements: Optional[Iterable[DimensionElement]] = None) -> None:
        """Export the dimension records associated with one or more data IDs.

        Parameters
        ----------
        dataIds : iterable of `DataCoordinate`.
            Fully-expanded data IDs to export.
        elements : iterable of `DimensionElement`, optional
            Dimension elements whose records should be exported.  If `None`,
            records for all dimensions will be exported.
        """
        if elements is None:
            elements = frozenset(element for element in self._registry.dimensions.getStaticElements()
                                 if element.hasTable() and element.viewOf is None)
        else:
            elements = frozenset(elements)
        for dataId in dataIds:
            for record in dataId.records.values():
                if record is not None and record.definition in elements:
                    self._records[record.definition].setdefault(record.dataId, record)

    def saveDatasets(self, refs: Iterable[DatasetRef], *,
                     elements: Optional[Iterable[DimensionElement]] = None,
                     rewrite: Optional[Callable[[FileDataset], FileDataset]] = None) -> None:
        """Export one or more datasets.

        This automatically exports any `DatasetType`, `~CollectionType.RUN`
        collections, and dimension records associated with the datasets.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            References to the datasets to export.  Their `DatasetRef.id`
            attributes must not be `None`.  Duplicates are automatically
            ignored.  Nested data IDs must have `DataCoordinate.hasRecords`
            return `True`.
        elements : iterable of `DimensionElement`, optional
            Dimension elements whose records should be exported; this is
            forwarded to `saveDataIds` when exporting the data IDs of the
            given datasets.
        rewrite : callable, optional
            A callable that takes a single `FileDataset` argument and returns
            a modified `FileDataset`.  This is typically used to rewrite the
            path generated by the datastore.  If `None`, the `FileDataset`
            returned by `Datastore.export` will be used directly.

        Notes
        -----
        At present, this only associates datasets with `~CollectionType.RUN`
        collections.  Other collections will be included in the export in the
        future (once `Registry` provides a way to look up that information).
        """
        dataIds = set()
        for ref in refs:
            # The query interfaces that are often used to generate the refs
            # passed here often don't remove duplicates, so do that here for
            # convenience.
            if ref.id in self._dataset_ids:
                continue
            dataIds.add(self._registry.expandDataId(ref.dataId))
            # `exports` is a single-element list here, because we anticipate
            # a future where more than just Datastore.export has a vectorized
            # API and we can pull this out of the loop.
            exports = self._datastore.export([ref], directory=self._directory, transfer=self._transfer)
            if rewrite is not None:
                exports = [rewrite(export) for export in exports]
            self._dataset_ids.add(ref.getCheckedId())
            assert ref.run is not None
            self._datasets[ref.datasetType, ref.run].extend(exports)
        self.saveDataIds(dataIds, elements=elements)

    def _finish(self) -> None:
        """Delegate to the backend to finish the export process.

        For use by `Butler.export` only.
        """
        for element in self._registry.dimensions.sorted(self._records.keys()):
            self._backend.saveDimensionData(element, *self._records[element].values())
        for (datasetType, run), records in self._datasets.items():
            self._backend.saveDatasets(datasetType, run, *records)
        self._backend.finish()
