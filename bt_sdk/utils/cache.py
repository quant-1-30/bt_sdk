# -*- coding : utf-8 -*-
"""
Created on Tue Mar 12 15:37:47 2019

@author: python
"""
import os
import pickle
import errno
import pandas as pd
from collections import MutableMapping
from functools import partial
from shutil import rmtree, move
from tempfile import mkdtemp, NamedTemporaryFile

from collections import Sequence
from itertools import compress
# 弱引用 以及沙盒函数需要研究一下 、gcc回收机制
from weakref import WeakKeyDictionary, ref
from threading import Lock
from functools import wraps
from toolz.sandbox import unzip


def ensure_directory(path):
    """
    Ensure that a directory named "path" exists.
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if os.path.isdir(path):
            return
        raise


# cacheObject --- bar_reader
class Expired(Exception):
    """
        mark a cacheobject has expired
    """


class CachedObject(object):
    """
    A simple struct for maintaining a cached object with an expiration date.

    Parameters
    ----------
    value : object
        The object to cache.
    expires : datetime-like []
        Expiration date of `value`. The cache is considered invalid for dates
        **strictly greater** than `expires`.
    """
    def __init__(self, value, expires):
        self._value = value
        self._expires = expires

    def unwrap(self, dts):
        """
        Get the cached value.
        dts: sessions
        dts : [start_date, end_date]

        Returns
        -------
        value : object
            The cached value.

        Raises
        ------
        Expired
            Raised when `dt` is greater than self.expires.
        """
        expires = self._expires
        if dts[0] < expires[0] or dts[-1] > expires[-1]:
            raise Expired(expires)
        return self._value

    def _unsafe_get_value(self):
        """You almost certainly shouldn't use this."""
        return self._value


class ExpiredCache(object):
    """
    A cache of multiple CachedObjects, which returns the wrapped the value
    or raises and deletes the CachedObject if the value has expired.

    Parameters
    ----------
    cache : dict-like, optional
        An instance of a dict-like object which needs to support at least:
        `__del__`, `__getitem__`, `__setitem__`
        If `None`, than a dict is used as a default.

    cleanup : callable, optional
        A method that takes a single argument, a cached object, and is called
        upon expiry of the cached object, prior to deleting the object. If not
        provided, defaults to a no-op.

    """
    def __init__(self):
        self._cache = {}
        # cleanup = lambda value_to_clean: None

    def get(self, key, dts):
        """Get the value of a cached object.

        Parameters
        ----------
        key : any
            The key to lookup.
        dts : datetime list e.g.[start, end]
            The time of the lookup.

        Returns
        -------
        result : any
            The value for ``key``.

        Raises
        ------
        KeyError
            Raised if the key is not in the cache or the value for the key
            has expired.
        """
        value = self._cache[key].unwrap(dts)
        return value

    def set(self, key, value, expiration_dt):
        """Adds a new key value pair to the cache.

        Parameters
        ----------
        key : sid
            Asset object sid attribute
        value : any
            The value to store under the name ``key``.
        expiration_dt : datetime
            When should this mapping expire? The cache is considered invalid
            for dates **strictly greater** than ``expiration_dt``.
        """
        self._cache[key] = CachedObject(value, expiration_dt)

    def remove(self, key):
        del self._cache[key]


class DummyMapping(object):
    """
    Dummy object used to provide a mapping interface for singular values.
    """
    def __init__(self, value):
        self._value = value

    def __getitem__(self, key):
        return self._value


class dataframe_cache(MutableMapping):
    """A disk-backed cache for dataframes.

    ``dataframe_cache`` is a mutable mapping from string names to pandas
    DataFrame objects.
    This object may be used as a context manager to delete the cache directory
    on exit.

    Parameters
    ----------
    path : str, optional
        The directory path to the cache. Files will be written as
        ``path/<keyname>``.
    lock : Lock, optional
        Thread lock for multithreaded/multiprocessed access to the cache.
        If not provided no locking will be used.
    clean_on_failure : bool, optional
        Should the directory be cleaned up if an exception is raised in the
        context manager.
    serialize : {'msgpack', 'pickle:<n>'}, optional
        How should the data be serialized. If ``'pickle'`` is passed, an
        optional pickle protocol can be passed like: ``'pickle:3'`` which says
        to use pickle protocol 3.

    Notes
    -----
    The syntax ``cache[:]`` will load all key:value pairs into memory as a
    dictionary.
    The cache uses a temporary file format that is subject to change between
    versions of zipline.
    """
    def __init__(self,
                 path=None,
                 lock=None,
                 clean_on_failure=True,
                 serialization='msgpack'):
        # create directory
        self.path = path if path is not None else mkdtemp()
        self.lock = lock if lock is not None else nop_context
        self.clean_on_failure = clean_on_failure

        if serialization == 'msgpack':
            self.serialize = pd.DataFrame.to_msgpack
            self.deserialize = pd.read_msgpack
            self._protocol = None
        else:
            s = serialization.split(':', 1)
            if s[0] != 'pickle':
                raise ValueError(
                    "'serialization' must be either 'msgpack' or 'pickle[:n]'",
                )
            self._protocol = int(s[1]) if len(s) == 2 else None

            self.serialize = self._serialize_pickle
            self.deserialize = partial(pickle.load, encoding='latin-1')

        ensure_directory(self.path)

    def _serialize_pickle(self, df, path):
        with open(path, 'wb') as f:
            pickle.dump(df, f, protocol=self._protocol)

    def _keypath(self, key):
        return os.path.join(self.path, key)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        if not (self.clean_on_failure or value is None):
            # we are not cleaning up after a failure and there was an exception
            return

        with self.lock:
            # shutil rmtree --- delete an entile directory tree
            rmtree(self.path)

    def __getitem__(self, key):
        # self.items() return key value
        if key == slice(None):
            return dict(self.items())

        with self.lock:
            try:
                with open(self._keypath(key), 'rb') as f:
                    return self.deserialize(f)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                raise KeyError(key)

    def __setitem__(self, key, value):
        with self.lock:
            self.serialize(value, self._keypath(key))

    def __delitem__(self, key):
        with self.lock:
            try:
                os.remove(self._keypath(key))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # raise a keyerror if this directory did not exist
                    raise KeyError(key)
                # reraise the actual oserror otherwise
                raise

    def __iter__(self):
        return iter(os.listdir(self.path))

    def __len__(self):
        return len(os.listdir(self.path))

    def __repr__(self):
        # repr 为函数
        return '<%s: keys={%s}>' % (
            type(self).__name__,
            ', '.join(map(repr, sorted(self))),
        )


class lazyproperty:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value


class _WeakArgs(Sequence):
    """
    Works with _WeakArgsDict to provide a weak cache for function args.
    When any of those args are gc'd, the pair is removed from the cache.
    """
    def __init__(self, items, dict_remove=None):
        def remove(selfref=ref(self), dict_remove=dict_remove):
            self = selfref()
            if self is not None and dict_remove is not None:
                dict_remove(self)

        self._items, self._selectors = unzip(self._try_ref(item, remove)
                                             for item in items)
        self._items = tuple(self._items)
        self._selectors = tuple(self._selectors)

    def __getitem__(self, index):
        return self._items[index]

    def __len__(self):
        return len(self._items)

    @staticmethod
    def _try_ref(item, callback):
        try:
            """ 
                Return a weak reference to object.
                If callback is provided and not None, and the returned weakref object is still alive, 
                the callback will be called when the object is about to be finalized; 
                the weak reference object will be passed as the only parameter to the callback; 
                the referent will no longer be available.
            """
            return ref(item, callback), True
        except TypeError:
            return item, False

    @property
    def alive(self):
        """
        itertools.compress(data, selectors)¶
        Make an iterator that filters elements from data returning only those
        that have a corresponding element in selectors that evaluates to True.
        """
        return all(item() is not None
                   for item in compress(self._items, self._selectors))

    def __eq__(self, other):
        return self._items == other._items

    def __hash__(self):
        try:
            return self.__hash
        except AttributeError:
            h = self.__hash = hash(self._items)
            return h


class _WeakArgsDict(WeakKeyDictionary, object):
    def __delitem__(self, key):
        del self.data[_WeakArgs(key)]

    def __getitem__(self, key):
        return self.data[_WeakArgs(key)]

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.data)

    def __setitem__(self, key, value):
        self.data[_WeakArgs(key, self._remove)] = value

    def __contains__(self, key):
        try:
            wr = _WeakArgs(key)
        except TypeError:
            return False
        return wr in self.data

    def pop(self, key, *args):
        return self.data.pop(_WeakArgs(key), *args)


def _weak_lru_cache(maxsize=100):
    """
    Users should only access the lru_cache through its public API:
    cache_info, cache_clear
    The internals of the lru_cache are encapsulated for thread safety and
    to allow the implementation to change.
    """
    def decorating_function(user_function):

        hits, misses = [0], [0]
        kwd_mark = (object(),)    # separates positional and keyword args
        lock = Lock()             # needed because OrderedDict isn't threadsafe

        if maxsize is None:
            cache = _WeakArgsDict()  # cache without ordering or size limit

            @wraps(user_function)
            def wrapper(*args, **kwds):
                key = args
                if kwds:
                    key += kwd_mark + tuple(sorted(kwds.items()))
                try:
                    result = cache[key]
                    hits[0] += 1
                    return result
                except KeyError:
                    pass
                result = user_function(*args, **kwds)
                cache[key] = result
                misses[0] += 1
                return result
        else:
            # ordered least recent to most recent
            cache = _WeakArgsOrderedDict()
            cache_popitem = cache.popitem
            cache_renew = cache.move_to_end

            @wraps(user_function)
            def wrapper(*args, **kwds):
                key = args
                if kwds:
                    key += kwd_mark + tuple(sorted(kwds.items()))
                with lock:
                    try:
                        result = cache[key]
                        cache_renew(key)    # record recent use of this key
                        hits[0] += 1
                        return result
                    except KeyError:
                        pass
                result = user_function(*args, **kwds)
                with lock:
                    cache[key] = result     # record recent use of this key
                    misses[0] += 1
                    if len(cache) > maxsize:
                        # purge least recently used cache entry
                        cache_popitem(False)
                return result

        def cache_info():
            """Report cache statistics"""
            with lock:
                return hits[0], misses[0], maxsize, len(cache)

        def cache_clear():
            """Clear the cache and cache statistics"""
            with lock:
                cache.clear()
                hits[0] = misses[0] = 0

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper

    return decorating_function
