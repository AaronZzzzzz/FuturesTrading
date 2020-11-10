# -*- coding: utf-8 -*-
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

from . import api
from rqdatac import fund

__all__ = ["fund"]
