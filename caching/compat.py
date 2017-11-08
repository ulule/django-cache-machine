from __future__ import unicode_literals

import django

from django.core.cache import cache as default_cache
from django.core.cache.backends.base import InvalidCacheBackendError

try:
    if django.VERSION[:2] >= (1, 7):
        from django.core.cache import caches
        cache = caches['cache_machine']
    else:
        from django.core.cache import get_cache
        cache = get_cache('cache_machine')
except (InvalidCacheBackendError, ValueError):
    cache = default_cache
