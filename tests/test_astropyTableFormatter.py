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

"""Tests for AstropyTableFormatter.
"""

import unittest
import tempfile
import os
import shutil
import numpy

from astropy.table import Table

from lsst.daf.butler import Butler, DatasetType

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class AstropyTableFormatterTestCase(unittest.TestCase):
    """Test for AstropyTableFormatter.
    """

    def setUp(self):
        base = os.environ.get("LSST_DAF_BUTLER_TEST_TMP", TESTDIR)
        self.root = tempfile.mkdtemp(dir=base)
        Butler.makeRepo(self.root)
        ints = [1, 2, 3]
        names = ['one', 'two', 'three']
        transcendentals = [3.14, 2.718, 0.643]
        self.table = Table([ints, names, transcendentals],
                           names=['ints', 'names', 'transcendentals'])

    def tearDown(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)
        del self.table

    def testAstropyTableFormatter(self):
        butler = Butler(self.root, run="testrun")
        datasetType = DatasetType("table", [], "AstropyTable",
                                  universe=butler.registry.dimensions)
        butler.registry.registerDatasetType(datasetType)
        ref = butler.put(self.table, datasetType)
        uri = butler.getURI(ref)
        self.assertEqual(uri.getExtension(), '.ecsv')
        table = butler.get('table')
        self.assertTrue(numpy.all(table == self.table))


if __name__ == "__main__":
    unittest.main()
