import collections

from caching import config
from caching.utils import byid


class Invalidator(object):
    def __init__(self, cache, logger, *args, **kwargs):
        self.cache = cache
        self.logger = logger

    def make_key(self, key):
        if key.startswith(config.CACHE_PREFIX):
            return key

        return '{}{}'.format(config.CACHE_PREFIX, key)

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
            self.logger.debug('deleting object keys: %s' % obj_keys)
            self.cache.delete_many(map(self.make_key, obj_keys))
        if flush_keys:
            self.logger.debug('clearing flush lists: %s' % flush_keys)
            self.clear_flush_lists(flush_keys)

    def cache_objects(self, model, objects, query_key, query_flush):
        # Add this query to the flush list of each object.  We include
        # query_flush so that other things can be cached against the queryset
        # and still participate in invalidation.
        flush_keys = [o.flush_key() for o in objects]

        flush_lists = collections.defaultdict(set)
        for key in flush_keys:
            self.logger.debug('adding %s to %s' % (query_flush, key))
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
                    self.logger.debug('related: adding %s to %s' % (obj_flush, key))
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
        self.logger.debug('in expand_flush_lists')
        obj_keys = set(obj_keys)
        search_keys = flush_keys = set(flush_keys)

        # Add other flush keys from the lists, which happens when a parent
        # object includes a foreign key.
        while 1:
            new_keys = set()
            for key in self.get_flush_lists(search_keys):
                if config.FLUSH_PREFIX in key:
                    new_keys.add(key)
                else:
                    obj_keys.add(key)
            if not new_keys:
                return obj_keys, flush_keys

            self.logger.debug('search for %s found keys %s' % (search_keys, new_keys))
            flush_keys.update(new_keys)
            search_keys = new_keys

    def add_to_flush_list(self, mapping):
        """Update flush lists with the {flush_key: [query_key,...]} map."""
        flush_lists = collections.defaultdict(set)
        flush_lists.update(self.cache.get_many(map(self.make_key, list(mapping.keys()))))
        for key, list_ in list(mapping.items()):
            if flush_lists[key] is None:
                flush_lists[key] = set(list_)
            else:
                flush_lists[key].update(list_)
        self.cache.set_many(map(self.make_key, flush_lists))

    def get_flush_lists(self, keys):
        """Return a set of object keys from the lists in `keys`."""
        return set(e for flush_list in
                   [_f for _f in list(self.cache.get_many(map(self.make_key, keys)).values()) if _f]
                   for e in flush_list)

    def clear_flush_lists(self, keys):
        """Remove the given keys from the database."""
        self.cache.delete_many(map(self.make_key, keys))
