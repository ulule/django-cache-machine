from __future__ import unicode_literals

import logging

from django.conf import settings


from caching import config
from caching.compat import cache
from caching.utils import load_class
from caching.invalidators import NullInvalidator, RedisInvalidator, Invalidator

logger = logging.getLogger('caching.invalidation')


if config.CACHE_MACHINE_NO_INVALIDATION:
    invalidator = NullInvalidator()
elif config.CACHE_MACHINE_USE_REDIS:
    invalidator = RedisInvalidator(cache=load_class(settings.REDIS_BACKEND)(**settings.REDIS_BACKEND_OPTIONS),
                                   logger=logger)
else:
    invalidator = Invalidator(cache=cache, logger=logger)
