#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any


class RemoteException(Exception):
    """
    RPC remote exception
    """

    def __init__(self, value: Any) -> None:
        """
        Constructor
        """
        self._value = value

    def __str__(self) -> str:
        """
        Output error message
        """
        return self._value
    