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

import logging
import os

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""


class CliLog:
    """Interface for managing python logging and ``lsst.log``.

    .. warning::

       When ``lsst.log`` is importable it is the primary logger, and
       ``lsst.log`` is set up to be a handler for python logging - so python
       logging will be processed by ``lsst.log``.

    This class defines log format strings for the log output and timestamp
    formats, for both ``lsst.log`` and python logging. If lsst.log is
    importable then the ``lsstLog_`` format strings will be used, otherwise
    the ``pylog_`` format strings will be used.

    This class can perform log uninitialization, which allows command line
    interface code that initializes logging to run unit tests that execute in
    batches, without affecting other unit tests. See ``resetLog``."""

    defaultLsstLogLevel = lsstLog.FATAL if lsstLog is not None else None

    lsstLog_longLogFmt = "%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c (%X{LABEL})(%F:%L)- %m%n"
    """The log format used when the lsst.log package is importable and the log
    is initialized with longlog=True."""

    lsstLog_normalLogFmt = "%c %p: %m%n"
    """The log format used when the lsst.log package is importable and the log
    is initialized with longlog=False."""

    pylog_longLogFmt = "%(levelname)s %(asctime)s %(name)s %(filename)s:%(lineno)s - %(message)s"
    """The log format used when the lsst.log package is not importable and the
    log is initialized with longlog=True."""

    pylog_longLogDateFmt = "%Y-%m-%dT%H:%M:%S%z"
    """The log date format used when the lsst.log package is not importable and
    the log is initialized with longlog=True."""

    pylog_normalFmt = "%(name)s %(levelname)s: %(message)s"
    """The log format used when the lsst.log package is not importable and the
    log is initialized with longlog=False."""

    configState = []
    """Configuration state. Contains tuples where first item in a tuple is
    a method and remaining items are arguments for the method.
    """

    _initialized = False
    _lsstLogHandler = None
    _componentSettings = []

    @classmethod
    def initLog(cls, longlog):
        """Initialize logging. This should only be called once per program
        execution. After the first call this will log a warning and return.

        If lsst.log is importable, will add its log handler to the python
        root logger's handlers.

        Parameters
        ----------
        longlog : `bool`
            If True, make log messages appear in long format, by default False.
        """
        if cls._initialized:
            # Unit tests that execute more than one command do end up
            # calling this function multiple times in one program execution,
            # so do log a debug but don't log an error or fail, just make the
            # re-initialization a no-op.
            log = logging.getLogger(__name__.partition(".")[2])
            log.debug("Log is already initialized, returning without re-initializing.")
            return
        cls._initialized = True

        if lsstLog is not None:
            # Initialize global logging config. Skip if the env var
            # LSST_LOG_CONFIG exists. The file it points to would already
            # configure lsst.log.
            if not os.path.isfile(os.environ.get("LSST_LOG_CONFIG", "")):
                lsstLog.configure_prop(_LOG_PROP.format(
                    cls.lsstLog_longLogFmt if longlog else cls.lsstLog_normalLogFmt))
            cls._recordComponentSetting(None)
            pythonLogger = logging.getLogger()
            pythonLogger.setLevel(logging.INFO)
            cls._lsstLogHandler = lsstLog.LogHandler()
            # Forward all Python logging to lsstLog
            pythonLogger.addHandler(cls._lsstLogHandler)
        else:
            cls._recordComponentSetting(None)
            if longlog:
                logging.basicConfig(level=logging.INFO,
                                    format=cls.pylog_longLogFmt,
                                    datefmt=cls.pylog_longLogDateFmt)
            else:
                logging.basicConfig(level=logging.INFO, format=cls.pylog_normalFmt)

        # also capture warnings and send them to logging
        logging.captureWarnings(True)

        # remember this call
        cls.configState.append((cls.initLog, longlog))

    @classmethod
    def getHandlerId(cls):
        """Get the id of the lsst.log handler added to the python logger.

        Used for unit testing to verify addition & removal of the lsst.log
        handler.

        Returns
        -------
        `id` or `None`
            The id of the handler that was added if one was added, or None.
        """
        return id(cls._lsstLogHandler)

    @classmethod
    def resetLog(cls):
        """Uninitialize the butler CLI Log handler and reset component log
        levels.

        If the lsst.log handler was added to the python root logger's handlers
        in `initLog`, it will be removed here.

        For each logger level that was set by this class, sets that logger's
        level to the value it was before this class set it. For lsst.log, if a
        component level was uninitialized, it will be set to
        `Log.defaultLsstLogLevel` because there is no log4cxx api to set a
        component back to an uninitialized state.
        """
        if cls._lsstLogHandler is not None:
            logging.getLogger().removeHandler(cls._lsstLogHandler)
        for componentSetting in reversed(cls._componentSettings):
            if lsstLog is not None and componentSetting.lsstLogLevel is not None:
                lsstLog.setLevel(componentSetting.component or "", componentSetting.lsstLogLevel)
            logger = logging.getLogger(componentSetting.component)
            logger.setLevel(componentSetting.pythonLogLevel)
        cls._setLogLevel(None, "INFO")
        cls._initialized = False
        cls.configState = []

    @classmethod
    def setLogLevels(cls, logLevels):
        """Set log level for one or more components or the root logger.

        Parameters
        ----------
        logLevels : `list` of `tuple`
            per-component logging levels, each item in the list is a tuple
            (component, level), `component` is a logger name or an empty string
            or `None` for root logger, `level` is a logging level name, one of
            CRITICAL, ERROR, WARNING, INFO, DEBUG (case insensitive).
        """
        if isinstance(logLevels, dict):
            logLevels = logLevels.items()

        # configure individual loggers
        for component, level in logLevels:
            cls._setLogLevel(component, level)
            # remember this call
            cls.configState.append((cls._setLogLevel, component, level))

    @classmethod
    def _setLogLevel(cls, component, level):
        """Set the log level for the given component. Record the current log
        level of the component so that it can be restored when resetting this
        log.

        Parameters
        ----------
        component : `str` or None
            The name of the log component or None for the root logger.
        level : `str`
            A valid python logging level.
        """
        cls._recordComponentSetting(component)
        if lsstLog is not None:
            lsstLogger = lsstLog.Log.getLogger(component or "")
            lsstLogger.setLevel(cls._getLsstLogLevel(level))
        logging.getLogger(component or None).setLevel(cls._getPyLogLevel(level))

    @staticmethod
    def _getPyLogLevel(level):
        """Get the numeric value for the given log level name.

        Parameters
        ----------
        level : `str`
            One of the python `logging` log level names.

        Returns
        -------
        numericValue : `int`
            The python `logging` numeric value for the log level.
        """
        return getattr(logging, level, None)

    @staticmethod
    def _getLsstLogLevel(level):
        """Get the numeric value for the given log level name.

        If `lsst.log` is not setup this function will return `None` regardless
        of input. `daf_butler` does not directly depend on `lsst.log` and so it
        will not be setup when `daf_butler` is setup. Packages that depend on
        `daf_butler` and use `lsst.log` may setup `lsst.log`.

        Will adapt the python name to an `lsst.log` name:
        - CRITICAL to FATAL
        - WARNING to WARN

        Parameters
        ----------
        level : `str`
            One of the python `logging` log level names.

        Returns
        -------
        numericValue : `int` or `None`
            The `lsst.log` numeric value.
        """
        if lsstLog is None:
            return None
        if level == "CRITICAL":
            level = "FATAL"
        elif level == "WARNING":
            level = "WARN"
        return getattr(lsstLog.Log, level, None)

    class ComponentSettings:
        """Container for log level values for a logging component."""
        def __init__(self, component):
            self.component = component
            self.pythonLogLevel = logging.getLogger(component).level
            self.lsstLogLevel = (lsstLog.Log.getLogger(component or "").getLevel()
                                 if lsstLog is not None else None)
            if self.lsstLogLevel == -1:
                self.lsstLogLevel = CliLog.defaultLsstLogLevel

        def __repr__(self):
            return (f"ComponentSettings(component={self.component}, pythonLogLevel={self.pythonLogLevel}, "
                    f"lsstLogLevel={self.lsstLogLevel})")

    @classmethod
    def _recordComponentSetting(cls, component):
        """Cache current levels for the given component in the list of
        component levels."""
        componentSettings = cls.ComponentSettings(component)
        cls._componentSettings.append(componentSettings)

    @classmethod
    def replayConfigState(cls, configState):
        """Re-create configuration using configuration state recorded earlier.

        Parameters
        ----------
        configState : `list` of `tuple`
            Tuples contain a method as first item and arguments for the method,
            in the same format as ``cls.configState``.
        """
        if cls._initialized or cls.configState:
            # Already initialized, do not touch anything.
            log = logging.getLogger(__name__.partition(".")[2])
            log.warning("Log is already initialized, will not replay configuration.")
            return

        # execute each one in order
        for call in configState:
            method, *args = call
            method(*args)
