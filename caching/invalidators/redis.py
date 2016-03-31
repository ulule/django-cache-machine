from .base import Invalidator


def get_redis_client(cache):
    client = getattr(cache, '_client', getattr(cache, 'master_client', None))

    if not client and hasattr(cache, 'client_list'):
        client = list(cache.client_list)[0]

    if not client:
        get_client = getattr(cache, 'get_client', None)

        if get_client and callable(get_client):
            client = get_client()

    return client


class RedisInvalidator(Invalidator):
    def __init__(self, cache, *args, **kwargs):
        self.client = get_redis_client(cache)

        if not self.client:
            raise NotImplementedError('We can\'t retrieve the client based on the Django cache instance')

        super(RedisInvalidator, self).__init__(cache, *args, **kwargs)

    def safe_key(self, key):
        if ' ' in key or '\n' in key:
            self.logger.warning('BAD KEY: "%s"' % key)
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
