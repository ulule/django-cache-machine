from __future__ import unicode_literals

import collections
import hashlib
import logging

try:
    import redis as redislib
except ImportError:
    redislib = None

from django.conf import settings
from django.utils import encoding, translation, six


from caching import config
from caching.compat import cache
from caching.utils import load_class

log = logging.getLogger('caching.invalidation')


def make_key(k, with_locale=True):
    """Generate the full key for ``k``, with a prefix."""
    key = encoding.smart_bytes('%s:%s' % (config.CACHE_PREFIX, k))
    if with_locale:
        key += encoding.smart_bytes(translation.get_language())
    # memcached keys must be < 250 bytes and w/o whitespace, but it's nice
    # to see the keys when using locmem.
    return hashlib.md5(key).hexdigest()


def flush_key(obj):
    """We put flush lists in the flush: namespace."""
    key = obj if isinstance(obj, six.string_types) else obj.get_cache_key(incl_db=False)
    return config.FLUSH + make_key(key, with_locale=False)


def byid(obj):
    key = obj if isinstance(obj, six.string_types) else obj.cache_key
    return make_key('byid:' + key)


class Invalidator(object):
    def invalidate_objects(self, objects, is_new_instance=False, model_cls=None):
        """Invalidate all the flush lists for the given ``objects``."""
        obj_keys = [k for o in objects for k in o._cache_keys()]
        flush_keys = [k for o in objects for k in o._flush_keys()]
        # If whole-model invalidation on create is enabled, include this model's
        # key in the list to be invalidated. Note that the key itself won't
        # contain anything in the cache, but its corresponding flush key will.
        if (config.CACHE_INVALIDATE_ON_CREATE == config.WHOLE_MODEL and
           is_new_instance and model_cls and hasattr(model_cls, 'model_flush_key')):
            flush_keys.append(model_cls.model_flush_key())
        if not obj_keys or not flush_keys:
            return
        obj_keys, flush_keys = self.expand_flush_lists(obj_keys, flush_keys)
        if obj_keys:
            log.debug('deleting object keys: %s' % obj_keys)
            cache.delete_many(obj_keys)
        if flush_keys:
            log.debug('clearing flush lists: %s' % flush_keys)
            self.clear_flush_lists(flush_keys)

    def cache_objects(self, model, objects, query_key, query_flush):
        # Add this query to the flush list of each object.  We include
        # query_flush so that other things can be cached against the queryset
        # and still participate in invalidation.
        flush_keys = [o.flush_key() for o in objects]

        flush_lists = collections.defaultdict(set)
        for key in flush_keys:
            log.debug('adding %s to %s' % (query_flush, key))
            flush_lists[key].add(query_flush)
        flush_lists[query_flush].add(query_key)
        # Add this query to the flush key for the entire model, if enabled
        model_flush = model.model_flush_key()
        if config.CACHE_INVALIDATE_ON_CREATE == config.WHOLE_MODEL:
            flush_lists[model_flush].add(query_key)
        # Add each object to the flush lists of its foreign keys.
        for obj in objects:
            obj_flush = obj.flush_key()
            for key in obj._flush_keys():
                if key not in (obj_flush, model_flush):
                    log.debug('related: adding %s to %s' % (obj_flush, key))
                    flush_lists[key].add(obj_flush)
                if config.FETCH_BY_ID:
                    flush_lists[key].add(byid(obj))
        self.add_to_flush_list(flush_lists)

    def expand_flush_lists(self, obj_keys, flush_keys):
        """
        Recursively search for flush lists and objects to invalidate.

        The search starts with the lists in `keys` and expands to any flush
        lists found therein.  Returns ({objects to flush}, {flush keys found}).
        """
        log.debug('in expand_flush_lists')
        obj_keys = set(obj_keys)
        search_keys = flush_keys = set(flush_keys)

        # Add other flush keys from the lists, which happens when a parent
        # object includes a foreign key.
        while 1:
            new_keys = set()
            for key in self.get_flush_lists(search_keys):
                if key.startswith(config.FLUSH):
                    new_keys.add(key)
                else:
                    obj_keys.add(key)
            if new_keys:
                log.debug('search for %s found keys %s' % (search_keys, new_keys))
                flush_keys.update(new_keys)
                search_keys = new_keys
            else:
                return obj_keys, flush_keys

    def add_to_flush_list(self, mapping):
        """Update flush lists with the {flush_key: [query_key,...]} map."""
        flush_lists = collections.defaultdict(set)
        flush_lists.update(cache.get_many(list(mapping.keys())))
        for key, list_ in list(mapping.items()):
            if flush_lists[key] is None:
                flush_lists[key] = set(list_)
            else:
                flush_lists[key].update(list_)
        cache.set_many(flush_lists)

    def get_flush_lists(self, keys):
        """Return a set of object keys from the lists in `keys`."""
        return set(e for flush_list in
                   [_f for _f in list(cache.get_many(keys).values()) if _f]
                   for e in flush_list)

    def clear_flush_lists(self, keys):
        """Remove the given keys from the database."""
        cache.delete_many(keys)


class RedisInvalidator(Invalidator):
    def __init__(self, client):
        self.client = client

    def safe_key(self, key):
        if ' ' in key or '\n' in key:
            log.warning('BAD KEY: "%s"' % key)
            return ''
        return key

    def add_to_flush_list(self, mapping):
        """Update flush lists with the {flush_key: [query_key,...]} map."""
        pipe = self.client.pipeline(transaction=False)
        for key, list_ in list(mapping.items()):
            for query_key in list_:
                # Redis happily accepts unicode, but returns byte strings,
                # so manually encode and decode the keys on the flush list here
                pipe.sadd(self.safe_key(key), query_key.encode('utf-8'))
        pipe.execute()

    def get_flush_lists(self, keys):
        flush_list = self.client.sunion(list(map(self.safe_key, keys)))
        return [k.decode('utf-8') for k in flush_list]

    def clear_flush_lists(self, keys):
        self.client.delete(*list(map(self.safe_key, keys)))


class NullInvalidator(Invalidator):
    def add_to_flush_list(self, mapping):
        return


def get_redis_backend():
    """Connect to redis from a string like CACHE_BACKEND."""
    return load_class(settings.REDIS_BACKEND)(**settings.REDIS_BACKEND_OPTIONS)


if config.CACHE_MACHINE_NO_INVALIDATION:
    invalidator = NullInvalidator()
elif config.CACHE_MACHINE_USE_REDIS:
    invalidator = RedisInvalidator(client=get_redis_backend())
else:
    invalidator = Invalidator()
