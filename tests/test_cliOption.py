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

"""Unit tests for the daf_butler dataset-type CLI option.
"""

import click
import click.testing
import unittest
import yaml

from lsst.daf.butler.cli.opt import config_file_option, config_option, dataset_type_option


class DatasetTypeTestCase(unittest.TestCase):

    @staticmethod
    @click.command()
    @dataset_type_option(help="the dataset type")
    def cli(dataset_type):
        click.echo(dataset_type, nl=False)

    def test_single(self):
        """test a single argument"""
        runner = click.testing.CliRunner()
        result = runner.invoke(DatasetTypeTestCase.cli, ["--dataset-type", "one"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one']")

    def test_multiple(self):
        """test multiple arguments, using the long and short option names"""
        runner = click.testing.CliRunner()
        result = runner.invoke(DatasetTypeTestCase.cli, ["--dataset-type", "one", "-d", "two"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two']")

    def test_singlePair(self):
        """test a single comma-separated value pair"""
        runner = click.testing.CliRunner()
        result = runner.invoke(DatasetTypeTestCase.cli, ["--dataset-type", "one,two"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two']")

    def test_multiplePair(self):
        """test multiple comma-separated value pairs"""
        runner = click.testing.CliRunner()
        result = runner.invoke(DatasetTypeTestCase.cli, ["--dataset-type", "one,two", "-d", "three,four"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two', 'three', 'four']")

    def test_help(self):
        """test capture of the help text"""
        runner = click.testing.CliRunner()
        result = runner.invoke(DatasetTypeTestCase.cli, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("the dataset type", result.stdout)


class ConfigTestCase(unittest.TestCase):

    @staticmethod
    @click.command()
    @config_option(help="foo bar baz")
    def cli(config):
        click.echo(yaml.dump(config), nl=False)

    def test_basic(self):
        """test arguments"""
        runner = click.testing.CliRunner()
        result = runner.invoke(ConfigTestCase.cli, ["--config", "a=1", "-c", "b=2,c=3"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertEqual(yaml.safe_load(result.stdout), dict(a="1", b="2", c="3"))

    def test_missing(self):
        @click.command()
        @config_option(required=True)
        def cli(config):
            pass
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, [])
        self.assertNotEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn('Missing option "-c" / "--config"', result.output)

    def test_help(self):
        """test capture of the help text"""
        runner = click.testing.CliRunner()
        result = runner.invoke(ConfigTestCase.cli, ["--help"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn("foo bar baz", result.stdout)


class ConfigFileTestCase(unittest.TestCase):

    @staticmethod
    @click.command()
    @config_file_option(help="foo bar baz")
    def cli(config_file):
        click.echo(config_file, nl=False)

    def test_basic(self):
        """test arguments"""
        runner = click.testing.CliRunner()
        result = runner.invoke(ConfigFileTestCase.cli, ["--config-file", "path/to/file"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertEqual("path/to/file", result.stdout)

    def test_missing(self):
        @click.command()
        @config_file_option(required=True)
        def cli(config):
            pass
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, [])
        self.assertNotEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn('Missing option "-C" / "--config-file"', result.output)

    def test_help(self):
        """test capture of the help text"""
        runner = click.testing.CliRunner()
        result = runner.invoke(ConfigFileTestCase.cli, ["--help"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn("foo bar baz", result.stdout)


if __name__ == "__main__":
    unittest.main()