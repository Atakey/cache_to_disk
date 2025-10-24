# -*- coding: utf-8 -*-
import hashlib
import inspect
import logging
import os
import pickle
import re
import threading
import time
import uuid
import warnings
from collections.abc import Mapping, Set
from datetime import datetime, timedelta
from functools import partial, wraps
from os import getenv, listdir, makedirs, remove
from os.path import (dirname, exists as file_exists, expanduser,
                     expandvars, getmtime, join as join_path, realpath)
from typing import Any, Callable, Dict, Optional, Tuple, Union

import orjson as json
from filelock import AcquireReturnProxy, FileLock as _FileLock, Timeout as FileLockTimeout

__all__ = [
    "cache_to_disk", "NoCacheCondition",
    "is_off_caching", "load_cache_metadata_json",
    "generate_cache_key", "cache_exists", "cache_function_value",
    "delete_disk_caches",
]

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

LOCK_SUFFIX = '.lock'
# Default cache entry lifetime in days.
DEFAULT_CACHE_AGE = 7
FILE_LOCK_TIMEOUT = int(getenv('DISK_CACHE_LOCK_TIMEOUT', 30))  # Seconds

# Cache directory and metadata file paths
DISK_CACHE_DIR = expanduser(expandvars(getenv('DISK_CACHE_DIR', join_path(dirname(realpath(__file__)), 'disk_cache'))))
DISK_CACHE_FILE = expanduser(
    expandvars(join_path(DISK_CACHE_DIR, getenv('DISK_CACHE_FILENAME', 'cache_to_disk_caches.json'))))
# Global thread lock for protecting concurrent access to DISK_CACHE_FILE
_thread_safe_singleton_lock = threading.Lock()

PathOrStr = Union[str, os.PathLike]


class NoCacheCondition(Exception):
    """
    Custom exception for a decorated function to raise to prevent caching on a per-call basis.

    The `function_value` kwarg can be set to return a specific value to the original caller
    instead of None.

    Example:
    @cache_to_disk(7)
    def network_query(hostname, port, query):
        response = b''
        try:
            # ... network operations ...
            response = read_data()
        except socket.error:
            # Return the partial response but do not cache the result.
            raise NoCacheCondition(function_value=response)
        return response
    """
    __slots__ = ['function_value']

    def __init__(self, function_value: Any = None):
        self.function_value = function_value


class FileLock(_FileLock):
    """
    Subclass of `filelock.FileLock` that adds a `read_only_ok` option.

    This allows the lock to be "acquired" in a degraded, read-only mode on
    filesystems where write permissions are not available, issuing a warning
    instead of raising an OSError.
    """

    def __init__(self, lock_file: PathOrStr, read_only_ok: bool = False, timeout: float = -1) -> None:
        super().__init__(str(lock_file), timeout=timeout)
        self._read_only_ok = read_only_ok

    def acquire(
            self,
            timeout: Optional[float] = None,
            poll_interval: float = 0.05,
            **kwargs
    ) -> Optional[AcquireReturnProxy]:
        try:
            return super().acquire(timeout=timeout, poll_interval=poll_interval, **kwargs)
        except OSError as err:
            # Handle permission denied (13), operation not permitted (1), or read-only filesystem (30).
            if err.errno not in (1, 13, 30):
                raise

            if file_exists(self.lock_file) and self._read_only_ok:
                warnings.warn(
                    f"Lacking permissions to create lock file '{self.lock_file}'. "
                    "Race conditions are possible if other processes are writing to the cache.",
                    UserWarning,
                )
                return None  # Proceed without a lock.
            else:
                raise


def is_off_caching() -> bool:
    """Checks if caching is globally disabled via the DISK_CACHE_MODE environment variable."""
    return getenv("DISK_CACHE_MODE", "on").lower() in {'off', '0'}


def generate_cache_key(func: Callable, *args: Any, **kwargs: Any) -> str:
    """
    Generates a deterministic cache key from a function and its arguments.

    The key is robust against non-functional code changes (e.g., comments,
    variable renames, docstrings) because it hashes the function's bytecode,
    closure, and constants, not its source code. Arguments are normalized
    to ensure that `f(a=1, b=2)` and `f(b=2, a=1)` produce the same key.
    """
    PICKLE_PROTOCOL = 4

    def _serialize_closure_safely(closure_cells):
        """Safely serialize closure contents, skipping problematic objects."""
        serializable_contents = []
        for cell in closure_cells:
            try:
                content = cell.cell_contents
                if isinstance(content, (str, int, float, bool, bytes, type(None))):
                    serializable_contents.append(content)
                elif isinstance(content, (tuple, list)):
                    if all(isinstance(x, (str, int, float, bool, bytes, type(None))) for x in content):
                        serializable_contents.append(content)
                else:
                    # For complex objects, just include their type signature
                    type_sig = f"{content.__class__.__module__}.{content.__class__.__name__}"
                    serializable_contents.append(f"<{type_sig}>")
            except Exception:
                # If we can't even access the cell contents, skip it
                serializable_contents.append("<inaccessible>")

        return pickle.dumps(tuple(serializable_contents), protocol=PICKLE_PROTOCOL)

    def _serialize(obj: Any) -> bytes:
        """Serializes an object into a deterministic byte representation for hashing."""
        # Handle functions and partials by inspecting their components.
        if isinstance(obj, partial):
            return b"partial:" + _serialize(obj.func) + _serialize(obj.args) + _serialize(obj.keywords)

        if inspect.isfunction(obj) or inspect.ismethod(obj):
            if hasattr(obj, '__code__'):
                # Identify the function by its module and name.
                func_identity = f"{obj.__module__}.{obj.__qualname__}".encode('utf-8')
                # Use the bytecode, which represents the function's logic.
                bytecode = obj.__code__.co_code
                # The docstring is often the first constant. Exclude it to make the key
                # immune to docstring changes.
                all_constants = obj.__code__.co_consts
                constants_to_serialize = all_constants
                if all_constants and all_constants[0] == obj.__doc__:
                    # If the first constant is the docstring, serialize the rest.
                    constants_to_serialize = all_constants[1:]
                constants = _serialize(constants_to_serialize)
                # Include variables from the enclosing scope (closure).
                closure = _serialize_closure_safely(obj.__closure__) if obj.__closure__ else b''
                return b"func:" + func_identity + bytecode + constants + closure

        # For collections, create a canonical representation by sorting.
        if isinstance(obj, Mapping):
            return pickle.dumps(sorted(obj.items()), protocol=PICKLE_PROTOCOL)
        if isinstance(obj, Set):
            return pickle.dumps(sorted(list(obj)), protocol=PICKLE_PROTOCOL)

        # MODIFIED: Enhanced fallback for unpickleable objects.
        try:
            return pickle.dumps(obj, protocol=PICKLE_PROTOCOL)
        except (pickle.PicklingError, TypeError):
            # Fallback for unpickleable objects like coroutines, sockets, etc.
            # repr() is often not stable as it may include memory addresses.
            # Instead, we use a stable representation based on the object's type and name if available.
            if inspect.iscoroutine(obj):
                # For coroutine objects, use their name for a stable representation.
                return f"<coroutine:{obj.__name__}>".encode('utf-8')

            # For other unpickleable types, use their type signature.
            type_sig = f"{obj.__class__.__module__}.{obj.__class__.__name__}"
            return f"<unpickleable:{type_sig}>".encode('utf-8')

    hasher = hashlib.sha256()
    # Hash the function's serialized representation.
    hasher.update(_serialize(func))
    # Normalize arguments to a canonical form (sorted by name) and hash them.
    try:
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        for name, value in sorted(bound_args.arguments.items()):
            hasher.update(name.encode('utf-8'))
            hasher.update(_serialize(value))
    except (TypeError, ValueError):
        # Fallback for callables where signature binding fails (e.g., some built-ins).
        hasher.update(b"fallback_args:")
        hasher.update(_serialize(args))
        hasher.update(b"fallback_kwargs:")
        hasher.update(_serialize(kwargs))

    digest = hasher.hexdigest()
    # Find the original function to get its name, unwrapping `partial` if necessary.
    original_func = func
    while isinstance(original_func, partial):
        original_func = original_func.func

    # Prefix the hash with the function's qualified name for better readability.
    try:
        func_name = original_func.__qualname__
    except AttributeError:
        # Fallback for callables that don't have __qualname__ (e.g., some C-level callables)
        func_name = "unknown_callable"
    safe_func_name = re.sub(r'[^a-zA-Z0-9_.]', '_', func_name)
    return f"{safe_func_name}_{digest}"


def _get_file_age(filename: PathOrStr, unit: str = 'days') -> int:
    """Returns the age of a file in the specified unit ('days' or 'seconds')."""
    if not file_exists(filename):
        return -1
    age = datetime.today() - datetime.fromtimestamp(getmtime(filename))
    if unit == 'days':
        return age.days
    elif unit == 'seconds':
        return int(age.total_seconds())
    raise ValueError(f"Unsupported unit: {unit}. Use 'days' or 'seconds'.")


def _ensure_dir(directory: PathOrStr) -> None:
    """Ensures that a directory and its parent directories exist."""
    makedirs(directory, exist_ok=True)


def _load_cache_metadata_json_no_lock(cache_file: PathOrStr) -> Dict[str, Any]:
    """Loads JSON metadata from a file without acquiring a lock. Caller must handle synchronization."""
    if not file_exists(cache_file):
        return {}
    try:
        with open(cache_file, 'rb') as f:
            return json.loads(f.read())
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Error loading cache metadata {cache_file}: {e}. Returning empty cache.")
        return {}


def _write_cache_file_no_lock(cache_metadata_dict: Dict[str, Any], cache_file: PathOrStr) -> None:
    """
    Writes JSON metadata to a file atomically using a temporary file and rename pattern.
    Caller must handle synchronization.
    """
    temp_file_path = f"{cache_file}.{uuid.uuid4()}.tmp"
    try:
        with open(temp_file_path, 'wb') as f:
            f.write(json.dumps(cache_metadata_dict))
        os.replace(temp_file_path, cache_file)
    except Exception as e:
        logger.error(f"Failed to write cache metadata to {cache_file} atomically: {e}")
        raise
    finally:
        if file_exists(temp_file_path):
            try:
                remove(temp_file_path)
            except OSError as e:
                logger.error(f"Failed to remove temporary cache file {temp_file_path}: {e}")


def load_cache_metadata_json(cache_file: PathOrStr = DISK_CACHE_FILE) -> Dict[str, Any]:
    """Loads cache metadata from a JSON file, protected by a file lock for safe concurrent reads."""
    try:
        with FileLock(cache_file + LOCK_SUFFIX, read_only_ok=True, timeout=FILE_LOCK_TIMEOUT):
            return _load_cache_metadata_json_no_lock(cache_file)
    except FileLockTimeout:
        logger.warning(f"Timeout acquiring read lock for {cache_file}. Loading without lock (risk of stale data).")
        return _load_cache_metadata_json_no_lock(cache_file)
    except Exception as e:
        logger.exception(f"Unexpected error loading cache metadata {cache_file}: {e}")
        return {}


def _update_cache_metadata_with_lock(
        update_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        cache_file: PathOrStr = DISK_CACHE_FILE
) -> None:
    """
    Atomically loads, updates, and writes cache metadata using a file lock and a thread lock.
    """
    with _thread_safe_singleton_lock:  # Protects against multiple threads trying to acquire file lock
        try:
            with FileLock(cache_file + LOCK_SUFFIX, read_only_ok=False, timeout=FILE_LOCK_TIMEOUT):
                current_metadata = _load_cache_metadata_json_no_lock(cache_file)
                updated_metadata = update_func(current_metadata)
                _write_cache_file_no_lock(updated_metadata, cache_file)
        except FileLockTimeout:
            logger.error(f"Timeout acquiring write lock for {cache_file}. Cache update failed.")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error updating cache metadata {cache_file}: {e}")
            raise


def _pickle_data(data: Any, file_path: PathOrStr) -> None:
    """Serializes and writes a Python object to a file using pickle."""
    with open(file_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


def _unpickle_data(file_path: PathOrStr) -> Any:
    """Reads and deserializes a Python object from a pickle file."""
    with open(file_path, 'rb') as f:
        return pickle.load(f)


def cache_exists(
        cache_metadata: Dict[str, Any],
        cache_key: str,
        cache_dir: PathOrStr = DISK_CACHE_DIR
) -> Tuple[bool, Optional[Any]]:
    """
    Checks if a valid, non-stale cache entry exists for a given key.

    Returns:
        A tuple of (bool, value). (True, cached_value) if a valid cache entry
        is found, otherwise (False, None).
    """
    if is_off_caching() or cache_key not in cache_metadata:
        return False, None

    function_cache = cache_metadata[cache_key]
    max_age_days = int(function_cache.get('max_age_days', DEFAULT_CACHE_AGE))
    file_name = join_path(cache_dir, function_cache['file_name'])
    if not file_exists(file_name):
        return False, None

    file_age = _get_file_age(file_name)
    if file_age >= max_age_days:
        return False, None

    try:
        function_value = _unpickle_data(file_name)
        return True, function_value
    except Exception as e:
        logger.error(f"Failed to unpickle {file_name} for key {cache_key}. Removing corrupted entry. Error: {e}")
        # If the file is corrupted, attempt to remove it to prevent future errors.
        try:
            remove(file_name)
        except OSError as remove_e:
            logger.error(f"Failed to remove corrupted cache file {file_name}: {remove_e}")
        return False, None


def cache_function_value(
        function_value: Any,
        n_days_to_cache: int,
        cache_key: str,
        cache_dir: PathOrStr = DISK_CACHE_DIR,
        cache_file_path: PathOrStr = DISK_CACHE_FILE
) -> None:
    """
    Caches a function's return value to a pickle file and updates the metadata.
    """
    if is_off_caching():
        return

    new_file_name = cache_key + '.pkl'
    full_file_path = join_path(cache_dir, new_file_name)
    try:
        _pickle_data(function_value, full_file_path)
    except Exception as e:
        logger.error(f"Failed to write cache value for key {cache_key} to {full_file_path}: {e}")
        return  # Abort if data cannot be written.

    def update_metadata_on_add(current_meta: Dict[str, Any]) -> Dict[str, Any]:
        current_meta[cache_key] = {
            'file_name': new_file_name,
            'max_age_days': n_days_to_cache,
            'timestamp': datetime.now().isoformat()
        }
        return current_meta

    try:
        _update_cache_metadata_with_lock(update_metadata_on_add, cache_file_path)
    except Exception as e:
        logger.error(
            f"Failed to update metadata for key {cache_key}. Cleaning up orphaned file {full_file_path}. Error: {e}")
        try:
            if file_exists(full_file_path):
                remove(full_file_path)
        except OSError as remove_e:
            logger.error(f"Failed to clean up orphaned file {full_file_path}: {remove_e}")


def cache_to_disk(
        n_days_to_cache: int = DEFAULT_CACHE_AGE,
        cache_prefix_key: str = "",
        force: bool = False,
        cache_threshold_secs: float = 0.0,
        func: Optional[Callable] = None
) -> Callable:
    """
    A decorator that caches a function's return value to disk.
    It is async-aware and can decorate both synchronous and asynchronous functions.

    Args:
        n_days_to_cache: Number of days to keep the cache entry. Must be > 0.
        cache_prefix_key: Optional string to prefix to the cache key, useful for namespacing.
        force: If True, re-run the function and update the cache, even if a valid entry exists.
        cache_threshold_secs: If > 0, only cache the result if the function takes longer
                              than this threshold to execute.
        func: The function to decorate. This is handled automatically by Python's decorator syntax.
    """
    if n_days_to_cache < 1:
        raise ValueError("n_days_to_cache must be >0")

    def decorator(f: Callable) -> Callable:
        if inspect.iscoroutinefunction(f):
            @wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if is_off_caching():
                    return await f(*args, **kwargs)

                base_cache_key = generate_cache_key(f, *args, **kwargs)
                computed_cache_key = f"{cache_prefix_key}_{base_cache_key}" if cache_prefix_key else base_cache_key

                if not force:
                    current_metadata = load_cache_metadata_json(DISK_CACHE_FILE)
                    is_cached, value = cache_exists(current_metadata, computed_cache_key, DISK_CACHE_DIR)
                    if is_cached:
                        return value

                try:
                    start_time = time.time()
                    value = await f(*args, **kwargs)
                    elapsed_time = time.time() - start_time
                except NoCacheCondition as err:
                    return err.function_value
                except Exception:
                    logger.exception(f"Error in decorated async function {f.__name__}. Caching skipped.")
                    raise

                if elapsed_time >= cache_threshold_secs:
                    cache_function_value(value, n_days_to_cache, computed_cache_key, DISK_CACHE_DIR, DISK_CACHE_FILE)

                return value

            return async_wrapper
        else:
            @wraps(f)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                if is_off_caching():
                    return f(*args, **kwargs)

                base_cache_key = generate_cache_key(f, *args, **kwargs)
                computed_cache_key = f"{cache_prefix_key}_{base_cache_key}" if cache_prefix_key else base_cache_key

                if not force:
                    current_metadata = load_cache_metadata_json(DISK_CACHE_FILE)
                    is_cached, value = cache_exists(current_metadata, computed_cache_key, DISK_CACHE_DIR)
                    if is_cached:
                        return value

                try:
                    start_time = time.time()
                    value = f(*args, **kwargs)
                    elapsed_time = time.time() - start_time
                except NoCacheCondition as err:
                    return err.function_value
                except Exception:
                    logger.exception(f"Error in decorated function {f.__name__}. Caching skipped.")
                    raise

                if elapsed_time >= cache_threshold_secs:
                    cache_function_value(value, n_days_to_cache, computed_cache_key, DISK_CACHE_DIR, DISK_CACHE_FILE)

                return value

            return sync_wrapper

    return decorator(func) if func else decorator


def _parse_datetime_input(dt_input: Optional[Union[str, datetime]]) -> Optional[datetime]:
    """
    Parses a datetime input which can be either a datetime object or a string.
    Supported string formats:
        - "YYYY/MM/DD HH:MM:SS"
        - "YYYY-MM-DD HH:MM:SS"
        - "YYYY/MM/DD" (defaults to 00:00:00)
        - "YYYY-MM-DD" (defaults to 00:00:00)
    """
    if dt_input is None:
        return None
    if isinstance(dt_input, datetime):
        return dt_input

    if isinstance(dt_input, str):
        dt_input = dt_input.strip()
        formats = [
            "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d", "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(dt_input, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unable to parse datetime string: {dt_input}")

    raise TypeError(f"Unsupported datetime input type: {type(dt_input)}")


def _generic_cache_cleanup(
        should_delete_func: Callable[[str, Dict[str, Any]], bool],
        cleanup_orphans: bool,
        cache_dir: PathOrStr,
        cache_file: PathOrStr,
) -> int:
    """
    Generic internal function to perform cache cleanup.

    This function atomically updates the metadata file by removing entries that
    match the `should_delete_func` predicate, and then deletes the corresponding
    .pkl files. It can also optionally clean up orphaned .pkl files.

    Args:
        should_delete_func: A callable that takes (cache_key, metadata_dict) and
                            returns True if the entry should be deleted.
        cleanup_orphans: If True, also remove orphan .pkl files not in metadata.
        cache_dir: The directory where cache files are stored.
        cache_file: The path to the cache metadata JSON file.

    Returns:
        The number of cache entries that were deleted.
    """
    deleted_items_meta = {}

    def update_and_clean_metadata(current_meta: Dict[str, Any]) -> Dict[str, Any]:
        nonlocal deleted_items_meta

        keys_to_delete = {
            key for key, meta in current_meta.items()
            if should_delete_func(key, meta)
        }

        # Store metadata of items to be deleted for later file removal
        deleted_items_meta = {k: current_meta[k] for k in keys_to_delete}

        # Create the new metadata dictionary
        new_meta = {k: v for k, v in current_meta.items() if k not in keys_to_delete}

        if cleanup_orphans and file_exists(cache_dir):
            valid_files = {meta.get('file_name') for meta in new_meta.values() if meta.get('file_name')}
            for file_in_dir in listdir(cache_dir):
                if file_in_dir.endswith('.pkl') and file_in_dir not in valid_files:
                    full_path = join_path(cache_dir, file_in_dir)
                    try:
                        remove(full_path)
                        logger.info(f"Removed orphaned cache file: {full_path}")
                    except OSError as e:
                        logger.error(f"Failed to remove orphaned cache file {full_path}: {e}")

        return new_meta

    try:
        _update_cache_metadata_with_lock(update_and_clean_metadata, cache_file)
    except Exception as e:
        logger.error(f"Failed to update cache metadata during cleanup: {e}")
        return 0

    # After metadata is updated, remove the corresponding cache files
    for key, meta in deleted_items_meta.items():
        file_name = meta.get('file_name')
        if file_name:
            file_path = join_path(cache_dir, file_name)
            if file_exists(file_path):
                try:
                    remove(file_path)
                except OSError as e:
                    logger.error(f"Failed to delete cache file {file_path} for key {key}: {e}")

    return len(deleted_items_meta)


def delete_disk_caches(
        prefix: Optional[str] = None,
        min_age: Optional[Union[str, datetime]] = None,
        max_age: Optional[Union[str, datetime]] = None,
        filter_func: Optional[Callable[[str, Dict[str, Any]], bool]] = None,
        cache_dir: PathOrStr = DISK_CACHE_DIR,
        cache_file: PathOrStr = DISK_CACHE_FILE,
) -> int:
    """
    Deletes cache entries based on specified criteria.
    """
    min_age_dt = _parse_datetime_input(min_age)
    max_age_dt = _parse_datetime_input(max_age)

    if all(arg is None for arg in [prefix, min_age_dt, max_age_dt, filter_func]):
        logger.info("delete_disk_caches called without any criteria, no action taken.")
        return 0

    def should_delete(key: str, meta: Dict[str, Any]) -> bool:
        # 1. Prefix check
        if prefix and not key.startswith(prefix):
            return False

        # 2. Age check
        if min_age_dt or max_age_dt:
            file_name = meta.get('file_name')
            if not file_name: return True  # Treat missing file as deletable

            file_path = join_path(cache_dir, file_name)
            if not file_exists(file_path): return True

            try:
                file_time = datetime.fromtimestamp(getmtime(file_path))
                if min_age_dt and file_time > min_age_dt: return False
                if max_age_dt and file_time < max_age_dt: return False
            except OSError:
                return False  # Cannot stat file, do not delete

        # 3. Custom filter check
        if filter_func:
            try:
                if not filter_func(key, meta):
                    return False
            except Exception as e:
                logger.error(f"Custom filter function failed for key '{key}': {e}")
                return False

        return True

    return _generic_cache_cleanup(should_delete, False, cache_dir, cache_file)


def _delete_old_disk_caches(
        days: int = 1,
        cache_dir: PathOrStr = DISK_CACHE_DIR,
        cache_file: PathOrStr = DISK_CACHE_FILE,
) -> None:
    """
    Performs routine cleanup of the cache directory.
    """
    try:
        # with FileLock(cache_file + LOCK_SUFFIX, read_only_ok=False, timeout=FILE_LOCK_TIMEOUT):
        if file_exists(cache_file):
            if datetime.now() - datetime.fromtimestamp(getmtime(cache_file)) < timedelta(days=days):
                return

        def should_delete_stale(key: str, meta: Dict[str, Any]) -> bool:
            max_age_days = int(meta.get('max_age_days', DEFAULT_CACHE_AGE))
            file_name = meta.get('file_name')
            if not file_name: return True

            full_path = join_path(cache_dir, file_name)
            if not file_exists(full_path): return True

            return _get_file_age(full_path) >= max_age_days

        _generic_cache_cleanup(should_delete_stale, True, cache_dir, cache_file)

    except FileLockTimeout:
        logger.warning(f"Could not acquire lock for disk cache cleanup on {cache_file}. Skipping.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred during disk cache cleanup: {e}")


_ensure_dir(DISK_CACHE_DIR)
_delete_old_disk_caches()
