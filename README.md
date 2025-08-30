# cache_to_disk

[![PyPI version](https://badge.fury.io/py/cache_to_disk.svg)](https://badge.fury.io/py/cache_to_disk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A powerful and robust Python decorator for caching function results to disk. It's designed as a simple, file-based solution to speed up time-consuming computations without external dependencies like Redis or a database.

This project is an enhanced and production-hardened version inspired by the original work from [sarenehan/cache_to_disk](https://github.com/sarenehan/cache_to_disk).

## Key Features

- **Simple & Effective**: Cache any function's output with a single decorator.
- **Async-Aware**: Seamlessly cache both synchronous and asynchronous functions.
- **Disk-Based Persistence**: Persists results directly to the file system, surviving script restarts.
- **Configurable Expiration**: Set a specific lifetime (in days) for each cached result.
- **Robust Concurrency**: Employs thread-safe and process-safe file locking with read-only fallback, preventing race conditions and ensuring data integrity even on read-only filesystems.
- **Performance Optimized**: Uses `orjson` for rapid metadata processing and atomic file writes, making it efficient even with a large number of cache entries.
- **Intelligent Caching**: Avoid caching results from very fast functions where I/O overhead would outweigh the benefits (`cache_threshold_secs`).
- **Smart Key Generation**: Generates deterministic cache keys based on function bytecode, closure, and arguments, making them robust against non-functional code changes (e.g., comments, variable renames, docstrings).
- **Flexible Control**:
    - **Force Refresh**: Bypass the cache and re-run the function on demand.
    - **Conditional Caching**: Prevent caching on a per-call basis by raising `NoCacheCondition`, ideal for handling errors or partial results without polluting your cache.
- **Automatic Cleanup**: Periodically cleans up stale cache files, orphaned data files, and old lock files to maintain a healthy cache directory.

## Installation

Install the package directly from PyPI:

```bash
pip install cache_to_disk
```

Or, install from GitHub for the latest version:

```bash
pip install git+https://github.com/Atakey/cache_to_disk.git@main#egg=cache_to_disk
```

## Basic Usage

Using `cache_to_disk` is as simple as adding a decorator to your function. The decorator automatically detects if the function is `async` and handles it appropriately.

```python
import time
from cache_to_disk import cache_to_disk

@cache_to_disk(n_days_to_cache=7)
def expensive_computation(x, y):
    """This function simulates a slow operation."""
    print(f"Performing expensive computation for ({x}, {y})...")
    time.sleep(2)
    return x * y

# The first call will execute the function and cache the result.
print("First call:")
result1 = expensive_computation(10, 20)
print(f"Result: {result1}")

# The second call with the same arguments will be instantaneous.
print("\nSecond call (from cache):")
result2 = expensive_computation(10, 20)
print(f"Result: {result2}")

# Example with an async function (requires an async context to run)
import asyncio

@cache_to_disk(n_days_to_cache=1)
async def async_data_fetch(url):
    print(f"Fetching data from {url} asynchronously...")
    await asyncio.sleep(1) # Simulate network delay
    return {"url": url, "data": "some_async_data"}

async def main():
    print("\nFirst async call:")
    data1 = await async_data_fetch("http://example.com/api/async")
    print(f"Async Result: {data1}")

    print("\nSecond async call (from cache):")
    data2 = await async_data_fetch("http://example.com/api/async")
    print(f"Async Result: {data2}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Usage

### Forcing a Cache Update

Use the `force=True` argument in the decorator to bypass the existing cache and re-run the function. The new result will update the cache.

```python
import time
from cache_to_disk import cache_to_disk

# This function will always re-run and update the cache due to force=True in the decorator
@cache_to_disk(n_days_to_cache=1, force=True)
def get_latest_data_always_forced():
    """Fetches the most recent data from a remote source, always forced."""
    print("Fetching latest data (always forced)...")
    time.sleep(1)
    return f"Data fetched at {time.time()}"

print("First call (always forced):")
print(get_latest_data_always_forced())

print("\nSecond call (still forced):")
print(get_latest_data_always_forced())
```

### Caching Only Slow Functions

The `cache_threshold_secs` parameter prevents caching for functions that execute too quickly, avoiding unnecessary disk I/O.

```python
import time
from cache_to_disk import cache_to_disk

@cache_to_disk(n_days_to_cache=1, cache_threshold_secs=0.5)
def potentially_fast_query(query_id, delay_secs):
    print(f"Executing query {query_id} with delay {delay_secs}s...")
    time.sleep(delay_secs)
    return f"Result for {query_id} after {delay_secs}s"

print("Query 1 (fast, will NOT cache):")
print(potentially_fast_query("Q1", 0.1)) # Less than 0.5s, won't cache

print("\nQuery 2 (slow, WILL cache):")
print(potentially_fast_query("Q2", 0.6)) # More than 0.5s, will cache

print("\nQuery 1 again (still not cached, will re-execute):")
print(potentially_fast_query("Q1", 0.1))

print("\nQuery 2 again (from cache):")
print(potentially_fast_query("Q2", 0.6))
```

### Conditionally Preventing Caching

You can raise the `NoCacheCondition` exception within your function to prevent a specific result from being cached. This is useful for handling errors or partial results without polluting your cache.

```python
import requests
from cache_to_disk import cache_to_disk, NoCacheCondition

@cache_to_disk(n_days_to_cache=1)
def query_api(endpoint):
    try:
        print(f"Attempting to query {endpoint}...")
        response = requests.get(endpoint, timeout=5)
        response.raise_for_status()  # Raise an exception for 4xx/5xx errors
        return response.json()
    except requests.exceptions.RequestException as e:
        # Don't cache the error, but return a default value to the caller.
        print(f"API call failed: {e}. Not caching this result.")
        raise NoCacheCondition(function_value={"error": "API unavailable", "details": str(e)})

# Example of a successful call (will cache)
print("--- Successful API Call ---")
try:
    result_success = query_api("https://jsonplaceholder.typicode.com/todos/1")
    print(f"Result: {result_success}")
    # Second call should be from cache
    print("\nSecond call (from cache):")
    result_cached = query_api("https://jsonplaceholder.typicode.com/todos/1")
    print(f"Result: {result_cached}")
except Exception as e:
    print(f"Unexpected error: {e}")

# Example of a failed call (will NOT cache)
print("\n--- Failed API Call ---")
try:
    result_fail = query_api("http://invalid.url.example.com/api/data")
    print(f"Result: {result_fail}")
    # Second call should re-execute, as it wasn't cached
    print("\nSecond call (should re-execute due to no cache):")
    result_fail_again = query_api("http://invalid.url.example.com/api/data")
    print(f"Result: {result_fail_again}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Customizing Cache Keys

Use `cache_prefix_key` to add a namespace to your cache keys. This is useful for preventing potential collisions between different functions or after making breaking changes to your function's logic.

```python
from cache_to_disk import cache_to_disk

@cache_to_disk(n_days_to_cache=30, cache_prefix_key="v2_user_data")
def get_user_profile(user_id):
    print(f"Fetching user profile for {user_id} (v2)...")
    return {"id": user_id, "name": f"User {user_id}", "version": "v2"}

@cache_to_disk(n_days_to_cache=30, cache_prefix_key="v1_user_data")
def get_user_profile_old(user_id):
    print(f"Fetching user profile for {user_id} (v1)...")
    return {"id": user_id, "name": f"User {user_id}", "version": "v1_legacy"}

print(get_user_profile(1))
print(get_user_profile(1)) # From cache (v2)

print(get_user_profile_old(1))
print(get_user_profile_old(1)) # From cache (v1)
```

## Clearing the Cache

The cache is stored in a local directory on your file system. To clear all caches, you can manually delete the cache directory. By default, it is located at `disk_cache` inside the library's installation directory.

You can find the installation path with:
```bash
pip show cache_to_disk
```
Then, navigate to the `Location` and delete the `disk_cache` folder.

Alternatively, you can set a custom cache directory via the `DISK_CACHE_DIR` environment variable and simply delete that directory.

## Configuration

You can configure the cache behavior using environment variables:

- **`DISK_CACHE_DIR`**: The base directory for storing cache files. Defaults to `disk_cache` within the package's installation directory. Example: `/tmp/my_app_cache`.
- **`DISK_CACHE_FILENAME`**: The filename for the main cache metadata JSON file. Defaults to `cache_to_disk_caches.json`.
- **`DISK_CACHE_MODE`**: Globally enables or disables caching. Set to `"off"` or `"0"` to disable caching for all decorated functions. Defaults to `"on"`.
- **`DISK_CACHE_LOCK_TIMEOUT`**: Timeout in seconds for acquiring a file lock. If a lock cannot be acquired within this time, a `FileLockTimeout` error is raised (or a warning issued for read-only operations). Defaults to `30` seconds.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
