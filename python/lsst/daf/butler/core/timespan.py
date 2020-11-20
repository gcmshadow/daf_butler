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

__all__ = (
    "Timespan",
    "TimespanDatabaseRepresentation",
)

from abc import abstractmethod
from typing import Any, ClassVar, Dict, Iterator, Mapping, NamedTuple, Optional, Tuple, Type, TypeVar, Union

import astropy.time
import astropy.utils.exceptions
import sqlalchemy
import warnings

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

from . import ddl
from .time_utils import astropy_to_nsec, EPOCH, MAX_TIME, times_equal
from ._topology import TopologicalExtentDatabaseRepresentation, TopologicalSpace


class Timespan(NamedTuple):
    """A 2-element named tuple for time intervals.

    Parameters
    ----------
    begin : ``Timespan``
        Minimum timestamp in the interval (inclusive).  `None` is interpreted
        as -infinity.
    end : ``Timespan``
        Maximum timestamp in the interval (exclusive).  `None` is interpreted
        as +infinity.
    """

    begin: Optional[astropy.time.Time]
    """Minimum timestamp in the interval (inclusive).

    `None` should be interpreted as -infinity.
    """

    end: Optional[astropy.time.Time]
    """Maximum timestamp in the interval (exclusive).

    `None` should be interpreted as +infinity.
    """

    def __str__(self) -> str:
        # Trap dubious year warnings in case we have timespans from
        # simulated data in the future
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            if self.begin is None:
                head = "(-∞, "
            else:
                head = f"[{self.begin.tai.isot}, "
            if self.end is None:
                tail = "∞)"
            else:
                tail = f"{self.end.tai.isot})"
        return head + tail

    def __repr__(self) -> str:
        # astropy.time.Time doesn't have an eval-friendly __repr__, so we
        # simulate our own here to make Timespan's __repr__ eval-friendly.
        tmpl = "astropy.time.Time('{t}', scale='{t.scale}', format='{t.format}')"
        begin = tmpl.format(t=self.begin) if self.begin is not None else None
        end = tmpl.format(t=self.end) if self.end is not None else None
        return f"Timespan(begin={begin}, end={end})"

    def __eq__(self, other: Any) -> bool:
        # Include some fuzziness in equality because round tripping
        # can introduce some drift at the picosecond level
        # Butler is okay wih nanosecond precision
        if not isinstance(other, type(self)):
            return False

        def compare_time(t1: Optional[astropy.time.Time], t2: Optional[astropy.time.Time]) -> bool:
            if t1 is None and t2 is None:
                return True
            if t1 is None or t2 is None:
                return False

            return times_equal(t1, t2)

        result = compare_time(self.begin, other.begin) and compare_time(self.end, other.end)
        return result

    def __ne__(self, other: Any) -> bool:
        # Need to override the explicit parent class implementation
        return not self.__eq__(other)

    def overlaps(self, other: Timespan) -> Any:
        """Test whether this timespan overlaps another.

        Parameters
        ----------
        other : `Timespan`
            Another timespan whose begin and end values can be compared with
            those of ``self`` with the ``>=`` operator, yielding values
            that can be passed to ``ops.or_`` and/or ``ops.and_``.

        Returns
        -------
        overlaps : `Any`
            The result of the overlap.  When ``ops`` is `operator`, this will
            be a `bool`.  If ``ops`` is `sqlachemy.sql`, it will be a boolean
            column expression.
        """
        return (
            (self.end is None or other.begin is None or self.end > other.begin)
            and (self.begin is None or other.end is None or other.end > self.begin)
        )

    def intersection(*args: Timespan) -> Optional[Timespan]:
        """Return a new `Timespan` that is contained by all of the given ones.

        Parameters
        ----------
        *args
            All positional arguments are `Timespan` instances.

        Returns
        -------
        intersection : `Timespan` or `None`
            The intersection timespan, or `None`, if there is no intersection
            or no arguments.
        """
        if len(args) == 0:
            return None
        elif len(args) == 1:
            return args[0]
        else:
            begins = [ts.begin for ts in args if ts.begin is not None]
            ends = [ts.end for ts in args if ts.end is not None]
            if not begins:
                begin = None
            elif len(begins) == 1:
                begin = begins[0]
            else:
                begin = max(*begins)
            if not ends:
                end = None
            elif len(ends) == 1:
                end = ends[0]
            else:
                end = min(*ends) if ends else None
            if begin is not None and end is not None and begin >= end:
                return None
            return Timespan(begin=begin, end=end)

    def difference(self, other: Timespan) -> Iterator[Timespan]:
        """Return the one or two timespans that cover the interval(s) that are
        in ``self`` but not ``other``.

        This is implemented as an iterator because the result may be zero, one,
        or two `Timespan` objects, depending on the relationship between the
        operands.

        Parameters
        ----------
        other : `Timespan`
            Timespan to subtract.

        Yields
        ------
        result : `Timespan`
            A `Timespan` that is contained by ``self`` but does not overlap
            ``other``.
        """
        if other.begin is not None:
            if self.begin is None or self.begin < other.begin:
                if self.end is not None and self.end < other.begin:
                    yield self
                else:
                    yield Timespan(begin=self.begin, end=other.begin)
        if other.end is not None:
            if self.end is None or self.end > other.end:
                if self.begin is not None and self.begin > other.end:
                    yield self
                else:
                    yield Timespan(begin=other.end, end=self.end)


_S = TypeVar("_S", bound="TimespanDatabaseRepresentation")


class TimespanDatabaseRepresentation(TopologicalExtentDatabaseRepresentation[Timespan]):
    """An interface that encapsulates how timespans are represented in a
    database engine.

    Most of this class's interface is comprised of classmethods.  Instances
    can be constructed via the `fromSelectable` method as a way to include
    timespan overlap operations in query JOIN or WHERE clauses.
    """

    NAME: ClassVar[str] = "timespan"
    SPACE: ClassVar[TopologicalSpace] = TopologicalSpace.TEMPORAL

    Compound: ClassVar[Type[TimespanDatabaseRepresentation]]
    """A concrete subclass of `TimespanDatabaseRepresentation` that simply
    uses two separate fields for the begin (inclusive) and end (excusive)
    endpoints.

    This implementation should be compatible with any SQL database, and should
    generally be used when a database-specific implementation is not available.
    """

    __slots__ = ()

    @abstractmethod
    def overlaps(self: _S, other: Union[Timespan, _S]) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing an overlap operation on
        timespans.

        Parameters
        ----------
        other : `Timespan` or `TimespanDatabaseRepresentation`
            The timespan to overlap ``self`` with; either a Python `Timespan`
            literal or an instance of the same `TimespanDatabaseRepresentation`
            as ``self``, representing a timespan in some other table or query
            within the same database.

        Returns
        -------
        overlap : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()


class _CompoundTimespanDatabaseRepresentation(TimespanDatabaseRepresentation):
    """An implementation of `TimespanDatabaseRepresentation` that simply stores
    the endpoints in two separate fields.

    This type should generally be accessed via
    `TimespanDatabaseRepresentation.Compound`, and should be constructed only
    via the `fromSelectable` method.

    Parameters
    ----------
    begin : `sqlalchemy.sql.ColumnElement`
        SQLAlchemy object representing the begin (inclusive) endpoint.
    end : `sqlalchemy.sql.ColumnElement`
        SQLAlchemy object representing the end (exclusive) endpoint.

    Notes
    -----
    ``NULL`` timespans are represented by having both fields set to ``NULL``;
    setting only one to ``NULL`` is considered a corrupted state that should
    only be possible if this interface is circumvented.  `Timespan` instances
    with one or both of `~Timespan.begin` and `~Timespan.end` set to `None`
    are set to fields mapped to the minimum and maximum value constants used
    by our integer-time mapping.
    """
    def __init__(self, begin: sqlalchemy.sql.ColumnElement, end: sqlalchemy.sql.ColumnElement, name: str):
        self.begin = begin
        self.end = end
        self._name = name

    __slots__ = ("begin", "end", "_name")

    @classmethod
    def makeFieldSpecs(cls, nullable: bool, name: Optional[str] = None, **kwargs: Any
                       ) -> Tuple[ddl.FieldSpec, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (
            ddl.FieldSpec(
                f"{name}_begin", dtype=ddl.AstropyTimeNsecTai, nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(astropy_to_nsec(EPOCH)))),
                **kwargs,
            ),
            ddl.FieldSpec(
                f"{name}_end", dtype=ddl.AstropyTimeNsecTai, nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(astropy_to_nsec(MAX_TIME)))),
                **kwargs,
            ),
        )

    @classmethod
    def getFieldNames(cls, name: Optional[str] = None) -> Tuple[str, ...]:
        # Docstring inherited.
        return (f"{cls.NAME}_begin", f"{cls.NAME}_end")

    @classmethod
    def update(cls, extent: Optional[Timespan], name: Optional[str] = None,
               result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        if result is None:
            result = {}
        if extent is None:
            begin = None
            end = None
        else:
            # These comparisons can trigger UTC -> TAI conversions that
            # can result in warnings for simulated data from the future
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
                if erfa is not None:
                    warnings.simplefilter("ignore", category=erfa.ErfaWarning)
                if extent.begin is None or extent.begin < EPOCH:
                    begin = EPOCH
                else:
                    begin = extent.begin
                # MAX_TIME is first in comparison to force a conversion
                # from the supplied time scale to TAI rather than
                # forcing MAX_TIME to be continually converted to
                # the target time scale (which triggers warnings to UTC)
                if extent.end is None or MAX_TIME <= extent.end:
                    end = MAX_TIME
                else:
                    end = extent.end
        result[f"{name}_begin"] = begin
        result[f"{name}_end"] = end
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any], name: Optional[str] = None) -> Optional[Timespan]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        begin = mapping[f"{name}_begin"]
        end = mapping[f"{name}_end"]
        if begin is None:
            if end is not None:
                raise RuntimeError(
                    f"Corrupted timespan extracted: begin is NULL, but end is {end}."
                )
            return None
        elif end is None:
            raise RuntimeError(
                f"Corrupted timespan extracted: end is NULL, but begin is {begin}."
            )
        if times_equal(begin, EPOCH):
            begin = None
        elif begin < EPOCH:
            raise RuntimeError(
                f"Corrupted timespan extracted: begin ({begin}) is before {EPOCH}."
            )
        if times_equal(end, MAX_TIME):
            end = None
        elif end > MAX_TIME:
            raise RuntimeError(
                f"Corrupted timespan extracted: end ({end}) is after {MAX_TIME}."
            )
        return Timespan(begin=begin, end=end)

    @classmethod
    def fromSelectable(cls, selectable: sqlalchemy.sql.FromClause,
                       name: Optional[str] = None) -> _CompoundTimespanDatabaseRepresentation:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return cls(begin=selectable.columns[f"{name}_begin"],
                   end=selectable.columns[f"{name}_end"],
                   name=name)

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return self.begin.is_(None)

    def overlaps(self, other: Union[Timespan, _CompoundTimespanDatabaseRepresentation]
                 ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, Timespan):
            begin = EPOCH if other.begin is None else other.begin
            end = MAX_TIME if other.end is None else other.end
        elif isinstance(other, _CompoundTimespanDatabaseRepresentation):
            begin = other.begin
            end = other.end
        else:
            raise TypeError(f"Unexpected argument to overlaps: {other!r}.")
        return sqlalchemy.sql.and_(self.end > begin, end > self.begin)

    def flatten(self, name: Optional[str] = None) -> Iterator[sqlalchemy.sql.ColumnElement]:
        # Docstring inherited.
        if name is None:
            yield self.begin
            yield self.end
        else:
            yield self.begin.label(f"{name}_begin")
            yield self.end.label(f"{name}_end")


TimespanDatabaseRepresentation.Compound = _CompoundTimespanDatabaseRepresentation
