from __future__ import unicode_literals

import django

from django.core.cache import cache as default_cache
from django.core.cache.backends.base import InvalidCacheBackendError

__all__ = ['DEFAULT_TIMEOUT', 'FOREVER']


if django.VERSION[:2] >= (1, 6):
    from django.core.cache.backends.base import DEFAULT_TIMEOUT as DJANGO_DEFAULT_TIMEOUT
    DEFAULT_TIMEOUT = DJANGO_DEFAULT_TIMEOUT
    FOREVER = None
else:
    DEFAULT_TIMEOUT = None
    FOREVER = 0


try:
    if django.VERSION[:2] >= (1, 7):
        from django.core.cache import caches
        cache = caches['cache_machine']
    else:
        from django.core.cache import get_cache
        cache = get_cache('cache_machine')
except (InvalidCacheBackendError, ValueError):
    cache = default_cache
