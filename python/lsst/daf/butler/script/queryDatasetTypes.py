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

from typing import Any, List

from .. import Butler
from ..core.utils import globToRegex


def queryDatasetTypes(repo, verbose, glob, components):
    """Get the dataset types in a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    verbose : `bool`
        If false only return the name of the dataset types. If false return
        name, dimensions, and storage class of each dataset type.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    components : `bool` or `None`
        If `True`, apply all glob patterns to component dataset type
        names as well.  If `False`, never apply patterns to components. If
        `None` (default), apply patterns to components only if their parent
        datasets were not matched by the expression. Fully-specified component
        datasets (`str` or `DatasetType` instances) are always included.

    Returns
    -------
    collections : `dict` [`str`, [`str`]]
        A dict whose key is "datasetTypes" and whose value is a list of
        collection names.
    """
    butler = Butler(repo)
    expression = globToRegex(glob)
    # Only pass expression to queryDatasetTypes if there is an expression to
    # apply; otherwise let queryDatasetTypes use its default value.
    kwargs = {}
    if expression:
        kwargs["expression"] = expression
    datasetTypes = butler.registry.queryDatasetTypes(components=components, **kwargs)
    info: List[Any]
    if verbose:
        info = [dict(name=datasetType.name,
                     dimensions=list(datasetType.dimensions.names),
                     storageClass=datasetType.storageClass.name)
                for datasetType in datasetTypes]
    else:
        info = [datasetType.name for datasetType in datasetTypes]
    return {"datasetTypes": info}
