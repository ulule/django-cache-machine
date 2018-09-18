import six
import hashlib

from django.utils import encoding, translation

from caching import config


def make_key(key, with_locale=True):
    """Generate the full key for ``k``, with a prefix."""
    if with_locale:
        key += translation.get_language()

    if not config.HASH_KEY:
        return key

    # memcached keys must be < 250 bytes and w/o whitespace, but it's nice
    # to see the keys when using locmem.
    return hashlib.md5(encoding.smart_bytes(key)).hexdigest()


def flush_key(obj):
    """We put flush lists in the flush: namespace."""
    key = obj if isinstance(obj, six.string_types) else obj.get_cache_key(incl_db=False)
    return config.FLUSH_PREFIX + make_key(key, with_locale=False)


def byid(obj):
    key = obj if isinstance(obj, six.string_types) else obj.cache_key
    return make_key("byid:" + key)
