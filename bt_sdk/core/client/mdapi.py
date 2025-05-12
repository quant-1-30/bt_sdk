# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple
from bt_sdk.utils.wrapper import singleton
from bt_sdk.core.client.api import Api


# @singleton
class MdApi(Api):
    params = (("protocol", "udp"),)

    def __init__(self, addr: Tuple[str, int]=()):
        self.addr = addr

    def getAddr(self):
        pass


# class Bars(object):

#     """A group of :class:`Bar` objects.

#     :param barDict: A map of instrument to :class:`Bar` objects.
#     :type barDict: map.

#     .. note::
#         All bars must have the same datetime.
#     """

#     def __init__(self, barDict):
#         if len(barDict) == 0:
#             raise Exception("No bars supplied")

#         # Check that bar datetimes are in sync
#         firstDateTime = None
#         firstInstrument = None
#         for instrument, currentBar in six.iteritems(barDict):
#             if firstDateTime is None:
#                 firstDateTime = currentBar.getDateTime()
#                 firstInstrument = instrument
#             elif currentBar.getDateTime() != firstDateTime:
#                 raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
#                     instrument,
#                     currentBar.getDateTime(),
#                     firstInstrument,
#                     firstDateTime
#                 ))

#         self.__barDict = barDict
#         self.__dateTime = firstDateTime

#     def __getitem__(self, instrument):
#         """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
#         If the instrument is not found an exception is raised."""
#         return self.__barDict[instrument]

#     def __contains__(self, instrument):
#         """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
#         return instrument in self.__barDict

#     def items(self):
#         return list(self.__barDict.items())

#     def keys(self):
#         return list(self.__barDict.keys())

#     def getInstruments(self):
#         """Returns the instrument symbols."""
#         return list(self.__barDict.keys())

#     def getDateTime(self):
#         """Returns the :class:`datetime.datetime` for this set of bars."""
#         return self.__dateTime

#     def getBar(self, instrument):
#         """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
#         return self.__barDict.get(instrument, None)


# class BasicBar(Bar):
#     # Optimization to reduce memory footprint.
#     __slots__ = (
#         '__dateTime',
#         '__open',
#         '__close',
#         '__high',
#         '__low',
#         '__volume',
#         '__adjClose',
#         '__frequency',
#         '__useAdjustedValue',
#         '__extra',
#     )

#     def __init__(self, dateTime, open_, high, low, close, volume, adjClose, frequency, extra={}):
#         if high < low:
#             raise Exception("high < low on %s" % (dateTime))
#         elif high < open_:
#             raise Exception("high < open on %s" % (dateTime))
#         elif high < close:
#             raise Exception("high < close on %s" % (dateTime))
#         elif low > open_:
#             raise Exception("low > open on %s" % (dateTime))
#         elif low > close:
#             raise Exception("low > close on %s" % (dateTime))

#         self.__dateTime = dateTime
#         self.__open = open_
#         self.__close = close
#         self.__high = high
#         self.__low = low
#         self.__volume = volume
#         self.__adjClose = adjClose
#         self.__frequency = frequency
#         self.__useAdjustedValue = False
#         self.__extra = extra

#     def __setstate__(self, state):
#         (self.__dateTime,
#             self.__open,
#             self.__close,
#             self.__high,
#             self.__low,
#             self.__volume,
#             self.__adjClose,
#             self.__frequency,
#             self.__useAdjustedValue,
#             self.__extra) = state

#     def __getstate__(self):
#         return (
#             self.__dateTime,
#             self.__open,
#             self.__close,
#             self.__high,
#             self.__low,
#             self.__volume,
#             self.__adjClose,
#             self.__frequency,
#             self.__useAdjustedValue,
#             self.__extra
#         )

#     def setUseAdjustedValue(self, useAdjusted):
#         if useAdjusted and self.__adjClose is None:
#             raise Exception("Adjusted close is not available")
#         self.__useAdjustedValue = useAdjusted

#     def getUseAdjValue(self):
#         return self.__useAdjustedValue

#     def getDateTime(self):
#         return self.__dateTime

#     def getOpen(self, adjusted=False):
#         if adjusted:
#             if self.__adjClose is None:
#                 raise Exception("Adjusted close is missing")
#             return self.__adjClose * self.__open / float(self.__close)
#         else:
#             return self.__open

#     def getHigh(self, adjusted=False):
#         if adjusted:
#             if self.__adjClose is None:
#                 raise Exception("Adjusted close is missing")
#             return self.__adjClose * self.__high / float(self.__close)
#         else:
#             return self.__high

#     def getLow(self, adjusted=False):
#         if adjusted:
#             if self.__adjClose is None:
#                 raise Exception("Adjusted close is missing")
#             return self.__adjClose * self.__low / float(self.__close)
#         else:
#             return self.__low

#     def getClose(self, adjusted=False):
#         if adjusted:
#             if self.__adjClose is None:
#                 raise Exception("Adjusted close is missing")
#             return self.__adjClose
#         else:
#             return self.__close

#     def getVolume(self):
#         return self.__volume

#     def getAdjClose(self):
#         return self.__adjClose

#     def getFrequency(self):
#         return self.__frequency

#     def getPrice(self):
#         if self.__useAdjustedValue:
#             return self.__adjClose
#         else:
#             return self.__close

#     def getExtraColumns(self):
#         return self.__extra


__all__ = ["MdApi"]
