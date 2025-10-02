import time
from os import PathLike
from typing import Any, Callable, Union
from diskcache import Cache, Lock, Timeout

DefaultParam = object()


class SafeDiskCache:
    @staticmethod
    def __init__cache(cache_dir):
        """Initialize a new Cache instance in the root directory with no eviction."""
        return Cache(directory=cache_dir, eviction_policy='none')

    def __retry__(self, retries: int = 3, backoff: float = 0.1, default: Any = False) -> Callable[
        ..., Union[bool, Any]]:
        """
        All operations already have one retry done by diskcache.
        This decorator adds additional retries for cache operations.

        :param int retries: Number of attempts before failing (default 3)
        :param float backoff: Backoff factor for retries (exponential) (default 0.1)
        :param Any default: Return value if all retries fail (default False)
        :return: Decorated function result or `default` on failure
        """

        def decorator(func: Callable):
            def wrapper(*func_args, **func_kwargs):
                for attempt in range(retries):
                    try:
                        return func(*func_args, **func_kwargs)
                    except Timeout:
                        time.sleep(backoff * (2 ** attempt))  # exponential backoff

                # Final attempt failed: reinitialize cache
                self.cache.close()
                self.cache = self.__init__cache(cache_dir=self.cache_dir)
                return default

            return wrapper

        return decorator

    def __init__(self, retries: int, cache_dir: PathLike, default: Any = None, ):
        """
        Initialize SafeDiskCache instance.

        :param int retries: Number of attempts before giving up (minimum 2)
        :param default: Default value returned if key not found (default None)
        """
        if retries < 2:
            raise RuntimeError("The minimum number of retries is two.")

        self.retries = retries - 1  # diskcache already does one retry
        self.default = default
        self.cache_dir = cache_dir
        self.cache = self.__init__cache(cache_dir=self.cache_dir)
        self.lock = Lock(self.cache, 'global_lock')

        # Wrap methods with retry decorator after initialization
        self.get = self.__retry__(self.retries)(self.get)
        self.put = self.__retry__(self.retries)(self.put)
        self.delete = self.__retry__(self.retries)(self.delete)

    def get(self, key: str, default=DefaultParam, retry: bool = True) -> Any:
        """
        Retrieve value from cache.

        :param str key: Key for item
        :param default: default value in case the key was not found (if not given self.default be used)
        :param bool retry: Retry if database timeout occurs (default True)
        :return: The value of the item or `default` if key was not in cache
        """
        if default is DefaultParam:
            default = self.default
        return self.cache.get(key=key, default=default, retry=retry)

    def put(self, key: str, value: Any, ttl: float = None, retry: bool = True) -> bool:
        """
        Sets key with value to cache.

        :param str key: Key for item
        :param value: Value for item
        :param float ttl: Seconds until item expires (default None, no expiry)
        :param bool retry: Retry if database timeout occurs (default True)

        :return: True if item was set, False otherwise
        """
        with self.lock:
            return self.cache.set(key=key, value=value, expire=ttl, retry=retry)

    def delete(self, key: str, retry: bool = True) -> bool:
        """
        Deletes the corresponding item for key from cache.
        Missing keys are ignored.

        :param str key: Key for item
        :param bool retry: Retry if database timeout occurs (default True)
        :return: True if the item was deleted, False otherwise
        """
        with self.lock:
            return self.cache.delete(key=key, retry=retry)
