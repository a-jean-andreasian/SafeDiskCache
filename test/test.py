from src.cache import SafeDiskCache
import os
import shutil


CACHE_DIR = os.path.join(os.getcwd(), "db_cache_test")

if os.path.exists(CACHE_DIR):
    shutil.rmtree(CACHE_DIR)

os.makedirs(CACHE_DIR, exist_ok=True)

def test_safe_disk_cache():
    cache = SafeDiskCache(retries=3, default="DEFAULT_VALUE")

    print("Putting key 'foo'")
    assert cache.put("foo", "bar") is True

    print("Getting key 'foo'")
    assert cache.get("foo") == "bar"

    print("Getting missing key without default")
    assert cache.get("missing_key") == "DEFAULT_VALUE"

    print("Getting missing key with explicit default")
    assert cache.get("missing_key", default="EXPLICIT") == "EXPLICIT"

    print("Deleting key 'foo'")
    assert cache.delete("foo") is True

    print("Getting deleted key")
    assert cache.get("foo") == "DEFAULT_VALUE"

    print("Test complete — all passed.")

if __name__ == "__main__":
    test_safe_disk_cache()
    shutil.rmtree(CACHE_DIR)
