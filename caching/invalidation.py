from __future__ import unicode_literals

import logging


from caching import config
from caching.compat import cache
from caching.invalidators import NullInvalidator, RedisInvalidator, Invalidator

logger = logging.getLogger('caching.invalidation')


if config.CACHE_MACHINE_NO_INVALIDATION:
    invalidator = NullInvalidator()
elif config.CACHE_MACHINE_USE_REDIS:
    invalidator = RedisInvalidator(cache=cache,
                                   logger=logger)
else:
    invalidator = Invalidator(cache=cache, logger=logger)
