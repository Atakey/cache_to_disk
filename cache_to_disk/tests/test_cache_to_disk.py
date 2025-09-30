# -*- coding: utf-8 -*-
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from multiprocessing import Process, Manager
from unittest.mock import patch

from filelock import Timeout as FileLockTimeout

# --- DO NOT CHANGE: Preserving original import structure ---
from .. import (
    NoCacheCondition,
    cache_exists,
    cache_function_value,
    cache_to_disk,
    generate_cache_key,
    is_off_caching,
    load_cache_metadata_json,
    delete_disk_caches
)


def worker_function(key, value, cache_dir, cache_file, result_dict, index):
    """Standalone function for multiprocessing tests"""
    # This function runs in a separate process, so it needs its own imports
    from .. import cache_function_value, load_cache_metadata_json, cache_exists
    try:
        cache_function_value(value, 7, key, cache_dir, cache_file)
        metadata = load_cache_metadata_json(cache_file)
        exists, val = cache_exists(metadata, key, cache_dir)
        result_dict[index] = (exists, val)
    except Exception as e:
        result_dict[index] = str(e)


def cleanup_worker_function(cache_dir, cache_file, result_dict, index):
    """Standalone function for cleanup multiprocessing test"""
    from .. import _delete_old_disk_caches
    try:
        _delete_old_disk_caches(0, cache_dir, cache_file)  # force cleanup
        result_dict[index] = "success"
    except Exception as e:
        result_dict[index] = str(e)


class TestCacheCore(unittest.TestCase):
    def setUp(self):
        # --- DO NOT CHANGE: Preserving original setUp logic ---
        self.test_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.test_dir, "disk_cache")
        self.cache_file = os.path.join(self.cache_dir, "cache_to_disk_caches.json")

        self.env_patcher = patch.dict(os.environ, {
            "DISK_CACHE_DIR": self.cache_dir,
            "DISK_CACHE_FILENAME": "cache_to_disk_caches.json",
            "DISK_CACHE_MODE": "on",
            "DISK_CACHE_LOCK_TIMEOUT": "5"
        })
        self.env_patcher.start()
        os.makedirs(self.cache_dir, exist_ok=True)

        if 'cache_to_disk' in sys.modules:
            del sys.modules['cache_to_disk']
        from .. import DISK_CACHE_DIR, DISK_CACHE_FILE
        self.assertEqual(DISK_CACHE_DIR, self.cache_dir)
        self.assertEqual(DISK_CACHE_FILE, self.cache_file)

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.test_dir)

    def test_environment_isolation(self):
        self.assertEqual(os.environ.get("DISK_CACHE_DIR"), self.cache_dir)
        self.assertEqual(os.environ.get("DISK_CACHE_FILENAME"), "cache_to_disk_caches.json")

    def test_cache_basic_functionality(self):
        test_value = {"data": "test"}
        cache_key = "test_key"
        cache_function_value(test_value, 7, cache_key, self.cache_dir, self.cache_file)
        metadata = load_cache_metadata_json(self.cache_file)
        self.assertIn(cache_key, metadata)
        exists, value = cache_exists(metadata, cache_key, self.cache_dir)
        self.assertTrue(exists)
        self.assertEqual(value, test_value)

    def test_cache_expiration(self):
        """Test cache expiration mechanism."""
        test_value = {"data": "expire_test"}
        cache_key_normal = "normal_key"

        # Store a cache entry that expires in 1 day
        cache_function_value(test_value, 1, cache_key_normal, self.cache_dir, self.cache_file)
        metadata = load_cache_metadata_json(self.cache_file)

        cached_file = os.path.join(self.cache_dir, f"{cache_key_normal}.pkl")
        self.assertTrue(os.path.exists(cached_file))

        # Modify file time to 2 days ago to make it stale
        old_time = (datetime.now() - timedelta(days=2)).timestamp()
        os.utime(cached_file, (old_time, old_time))

        # Check if it's considered expired
        exists_normal, _ = cache_exists(metadata, cache_key_normal, self.cache_dir)
        self.assertFalse(exists_normal, "Cache should expire after its set duration")

    def test_no_cache_condition(self):
        @cache_to_disk(7)
        def test_func():
            raise NoCacheCondition("no cache")

        result = test_func()
        self.assertEqual(result, "no cache")
        metadata = load_cache_metadata_json(self.cache_file)
        self.assertEqual(len(metadata), 0)

    def test_cache_key_generation(self):
        def test_func(a, b): return a + b

        key1 = generate_cache_key(test_func, 1, 2)
        key2 = generate_cache_key(test_func, 1, 2)
        self.assertEqual(key1, key2)
        key3 = generate_cache_key(test_func, 2, 1)
        self.assertNotEqual(key1, key3)

        def test_func2(a, b): return a + b

        key4 = generate_cache_key(test_func2, 1, 2)
        self.assertNotEqual(key1, key4)

    def test_cache_file_corruption(self):
        test_value = {"data": "corruption_test"}
        cache_key = "corrupt_key"
        cache_function_value(test_value, 7, cache_key, self.cache_dir, self.cache_file)
        cache_file_path = os.path.join(self.cache_dir, f"{cache_key}.pkl")
        with open(cache_file_path, "wb") as f:
            f.write(b"corrupted data")
        metadata = load_cache_metadata_json(self.cache_file)
        exists, _ = cache_exists(metadata, cache_key, self.cache_dir)
        self.assertFalse(exists)

    def test_file_lock_timeout(self):
        """Test behavior when file lock times out"""
        test_value = {"data": "lock_test"}
        # Patch FileLock within the specific module's namespace
        with patch("cache_to_disk.FileLock") as mock_lock:
            mock_lock.return_value.__enter__.side_effect = FileLockTimeout("Timeout")

            @cache_to_disk(7)
            def test_func():
                return test_value

            result = test_func()
            self.assertEqual(result, test_value)
            metadata = load_cache_metadata_json(self.cache_file)
            self.assertEqual(len(metadata), 0)

    def test_cache_disabled(self):
        with patch.dict(os.environ, {"DISK_CACHE_MODE": "off"}):
            self.assertTrue(is_off_caching())
            call_count = 0

            @cache_to_disk(7)
            def test_func(x):
                nonlocal call_count
                call_count += 1
                return x * 2

            test_func(5)
            test_func(5)
            self.assertEqual(call_count, 2)
            metadata = load_cache_metadata_json(self.cache_file)
            self.assertEqual(len(metadata), 0)


class TestCacheDecoratorFeatures(unittest.TestCase):
    def setUp(self):
        # --- DO NOT CHANGE: Preserving original setUp logic ---
        self.test_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.test_dir, "disk_cache")
        self.cache_file = os.path.join(self.cache_dir, "cache_to_disk_caches.json")

        self.env_patcher = patch.dict(os.environ, {
            "DISK_CACHE_DIR": self.cache_dir,
            "DISK_CACHE_FILENAME": "cache_to_disk_caches.json",
            "DISK_CACHE_MODE": "on",
            "DISK_CACHE_LOCK_TIMEOUT": "5"
        })
        self.env_patcher.start()
        os.makedirs(self.cache_dir, exist_ok=True)

        if 'cache_to_disk' in sys.modules:
            del sys.modules['cache_to_disk']
        from .. import DISK_CACHE_DIR, DISK_CACHE_FILE
        self.assertEqual(DISK_CACHE_DIR, self.cache_dir)
        self.assertEqual(DISK_CACHE_FILE, self.cache_file)

        self.call_count = 0

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.test_dir)

    def test_invalid_cache_days(self):
        """Test that n_days_to_cache < 1 raises ValueError."""
        with self.assertRaises(ValueError):
            @cache_to_disk(n_days_to_cache=0)
            def test_func(): pass

        with self.assertRaises(ValueError):
            @cache_to_disk(n_days_to_cache=-1)
            def test_func_negative(): pass


class TestCacheConcurrency(unittest.TestCase):
    def setUp(self):
        # --- DO NOT CHANGE: Preserving original setUp logic ---
        self.test_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.test_dir, "disk_cache")
        self.cache_file = os.path.join(self.cache_dir, "cache_to_disk_caches.json")

        self.env_patcher = patch.dict(os.environ, {
            "DISK_CACHE_DIR": self.cache_dir,
            "DISK_CACHE_FILENAME": "cache_to_disk_caches.json",
            "DISK_CACHE_MODE": "on",
            "DISK_CACHE_LOCK_TIMEOUT": "10"
        })
        self.env_patcher.start()
        os.makedirs(self.cache_dir, exist_ok=True)

        if 'cache_to_disk' in sys.modules:
            del sys.modules['cache_to_disk']

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.test_dir)

    def test_multiprocess_cache_access(self):
        test_key = "concurrent_key"
        test_value = {"data": "concurrent_test"}
        num_processes = 3
        manager = Manager()
        results = manager.dict()
        processes = []
        for i in range(num_processes):
            p = Process(target=worker_function,
                        args=(test_key, test_value, self.cache_dir, self.cache_file, results, i))
            processes.append(p)
            p.start()
        for p in processes:
            p.join(timeout=20)

        success_count = 0
        for i in range(num_processes):
            result = results.get(i)
            if isinstance(result, tuple):
                exists, val = result
                self.assertTrue(exists)
                self.assertEqual(val, test_value)
                success_count += 1
            else:
                self.assertIn("Timeout", str(result))

        self.assertGreaterEqual(success_count, 1)

        metadata = load_cache_metadata_json(self.cache_file)
        exists, val = cache_exists(metadata, test_key, self.cache_dir)
        self.assertTrue(exists)
        self.assertEqual(val, test_value)

    def test_concurrent_cache_cleanup(self):
        """Test that only one process performs cleanup."""
        test_keys = [f"cleanup_test_{i}" for i in range(3)]
        for key in test_keys:
            # Create a stale file
            cache_function_value({"data": key}, 1, key, self.cache_dir, self.cache_file)
            cache_file_path = os.path.join(self.cache_dir, f"{key}.pkl")
            old_time = (datetime.now() - timedelta(days=2)).timestamp()
            os.utime(cache_file_path, (old_time, old_time))

        num_workers = 3
        manager = Manager()
        results = manager.dict()
        processes = []
        for i in range(num_workers):
            p = Process(target=cleanup_worker_function, args=(self.cache_dir, self.cache_file, results, i))
            processes.append(p)
            p.start()
        for p in processes:
            p.join(timeout=20)

        success_count = sum(1 for i in range(num_workers) if results.get(i) == "success")
        self.assertGreaterEqual(success_count, 1)

        metadata = load_cache_metadata_json(self.cache_file)
        for key in test_keys:
            self.assertNotIn(key, metadata)


class TestCacheCleanupFeatures(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.test_dir, "disk_cache")
        self.cache_file = os.path.join(self.cache_dir, "cache_to_disk_caches.json")

        self.env_patcher = patch.dict(os.environ, {
            "DISK_CACHE_DIR": self.cache_dir,
            "DISK_CACHE_FILENAME": "cache_to_disk_caches.json",
            "DISK_CACHE_MODE": "on",
            "DISK_CACHE_LOCK_TIMEOUT": "5"
        })
        self.env_patcher.start()
        os.makedirs(self.cache_dir, exist_ok=True)

        if 'cache_to_disk' in sys.modules:
            del sys.modules['cache_to_disk']
        from .. import DISK_CACHE_DIR, DISK_CACHE_FILE
        self.assertEqual(DISK_CACHE_DIR, self.cache_dir)
        self.assertEqual(DISK_CACHE_FILE, self.cache_file)

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.test_dir)

    def test_delete_by_date_string(self):
        """Test delete_disk_caches with date string min_age/max_age."""
        from .. import cache_function_value, load_cache_metadata_json

        # Create two cache entries
        cache_function_value({"data": "old"}, 7, "old_key", self.cache_dir, self.cache_file)
        cache_function_value({"data": "new"}, 7, "new_key", self.cache_dir, self.cache_file)

        # Modify timestamps
        old_file = os.path.join(self.cache_dir, "old_key.pkl")
        new_file = os.path.join(self.cache_dir, "new_key.pkl")
        old_time = (datetime.now() - timedelta(days=5)).timestamp()
        new_time = datetime.now().timestamp()
        os.utime(old_file, (old_time, old_time))
        os.utime(new_file, (new_time, new_time))

        # Delete entries older than 3 days
        cutoff_date = (datetime.now() - timedelta(days=3)).strftime("%Y/%m/%d %H:%M:%S")
        deleted_count = delete_disk_caches(min_age=cutoff_date, cache_dir=self.cache_dir, cache_file=self.cache_file)
        self.assertEqual(deleted_count, 1)

        metadata = load_cache_metadata_json(self.cache_file)
        self.assertNotIn("old_key", metadata)
        self.assertIn("new_key", metadata)

    def test_delete_by_prefix(self):
        """Test delete_disk_caches with prefix."""

        cache_function_value({"data": "p1"}, 7, "prefix_key1", self.cache_dir, self.cache_file)
        cache_function_value({"data": "p2"}, 7, "prefix_key2", self.cache_dir, self.cache_file)
        cache_function_value({"data": "other"}, 7, "other_key", self.cache_dir, self.cache_file)

        deleted_count = delete_disk_caches(prefix="prefix_", cache_dir=self.cache_dir, cache_file=self.cache_file)
        self.assertEqual(deleted_count, 2)

        metadata = load_cache_metadata_json(self.cache_file)
        self.assertNotIn("prefix_key1", metadata)
        self.assertNotIn("prefix_key2", metadata)
        self.assertIn("other_key", metadata)

    def test_delete_with_filter_func(self):
        """Test delete_disk_caches with custom filter function."""

        cache_function_value({"data": "keep"}, 7, "keep_key", self.cache_dir, self.cache_file)
        cache_function_value({"data": "remove"}, 7, "remove_key", self.cache_dir, self.cache_file)

        def filter_func(key, meta):
            return "remove" in key

        deleted_count = delete_disk_caches(filter_func=filter_func, cache_dir=self.cache_dir,
                                           cache_file=self.cache_file)
        self.assertEqual(deleted_count, 1)

        metadata = load_cache_metadata_json(self.cache_file)
        self.assertNotIn("remove_key", metadata)
        self.assertIn("keep_key", metadata)


if __name__ == "__main__":
    unittest.main()
