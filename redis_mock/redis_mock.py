#:coding=utf-8:

import contextlib
try:
    import threading
except ImportError:
    import dummy_threading as threading

from redis import (
    Redis as BaseRedis,
    RedisError,
    ResponseError,
)
from redis.client import list_or_args

__all__ = (
    'Redis',
    'Pipeline',
    'RedisError',
    'ResponseError',
)

# Global in memory caches of data and
# locks for each redis connection
_caches = {}
_locks = {}

class RWLock(object):
    """
    Classic implementation of reader-writer lock with preference to writers.

    Readers can access a resource simultaneously.
    Writers get an exclusive access.

    API is self-descriptive:
        reader_enters()
        reader_leaves()
        writer_enters()
        writer_leaves()
    """
    def __init__(self):
        self.mutex     = threading.RLock()
        self.can_read  = threading.Semaphore(0)
        self.can_write = threading.Semaphore(0)
        self.active_readers  = 0
        self.active_writers  = 0
        self.waiting_readers = 0
        self.waiting_writers = 0

    def reader_enters(self):
        with self.mutex:
            if self.active_writers == 0 and self.waiting_writers == 0:
                self.active_readers += 1
                self.can_read.release()
            else:
                self.waiting_readers += 1
        self.can_read.acquire()

    def reader_leaves(self):
        with self.mutex:
            self.active_readers -= 1
            if self.active_readers == 0 and self.waiting_writers != 0:
                self.active_writers  += 1
                self.waiting_writers -= 1
                self.can_write.release()

    @contextlib.contextmanager
    def reader(self):
        self.reader_enters()
        try:
            yield
        finally:
            self.reader_leaves()

    def writer_enters(self):
        with self.mutex:
            if self.active_writers == 0 and self.waiting_writers == 0 and self.active_readers == 0:
                self.active_writers += 1
                self.can_write.release()
            else:
                self.waiting_writers += 1
        self.can_write.acquire()

    def writer_leaves(self):
        with self.mutex:
            self.active_writers -= 1
            if self.waiting_writers != 0:
                self.active_writers  += 1
                self.waiting_writers -= 1
                self.can_write.release()
            elif self.waiting_readers != 0:
                t = self.waiting_readers
                self.waiting_readers = 0
                self.active_readers += t
                while t > 0:
                    self.can_read.release()
                    t -= 1

    @contextlib.contextmanager
    def writer(self):
        self.writer_enters()
        try:
            yield
        finally:
            self.writer_leaves()

class MockConnectionPool(object):
    def disconnect(self):
        pass

class Redis(BaseRedis):
    """
    A redis.py mock object.
    """
    def __init__(self, host='localhost', port=6379, db=0, *args, **kwargs):
        self._name = '%s:%s:%s' % (host, port, db)
        self._host = host
        self._port = port
        self._charset = kwargs.pop('charset', 'utf-8')
        self._errors = kwargs.pop('errors', 'strict')
        self._server_version = kwargs.pop('_server_version', (2, 4))

        global _caches, _locks
        self._cache = _caches.setdefault(self._name, {})
        self._lock = _locks.setdefault(self._name, RWLock())
        self.connection = None
        self.connection_pool = MockConnectionPool()

    #### BASIC KEY COMMANDS ####
    def exists(self, name):
        def _exists(name):
            with self._lock.reader():
                name = self._to_str(name)
                return name in self._cache
        return self._execute_command(_exists, name)

    def get(self, name):
        def _get(name):
            with self._lock.reader():
                name = self._to_str(name)
                return self._assert_str(self._cache.get(name, None))
        return self._execute_command(_get, name)

    def getset(self, name, value):
        name = self._to_str(name)
        return self._execute_command(self.__set, name, value, _get=True)

    def incr(self, name, amount=1):
        def _incr(name, amount):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._assert_int(self._cache.get(name, None))
                value += self._assert_int(amount)
                value = self._to_str(value)
                self._cache[name] = value
                return value
        return self._execute_command(_incr, name, amount)

    def incrby(self, name, amount=1):
        name = self._to_str(name)
        return self.incr(name, amount)

    def set(self, name, value):
        name = self._to_str(name)
        return self._execute_command(self.__set, name, value)

    def setnx(self, name, value):
        name = self._to_str(name)
        return self._execute_command(self.__set, name, value, _nx=True)

    def __set(self, name, value, _nx=False, _get=False):
        """
        Non-pipelineable set operation.
        Used only internally.
        """
        with self._lock.writer():
            name = self._to_str(name)
            value = self._to_str(value)
            prev_value = self._cache.get(name, None)
            if _nx and name in self._cache:
                return prev_value if _get else False
            self._cache[name] = value
            return prev_value if _get else True

    def delete(self, *names):
        def _delete(*names):
            with self._lock.writer():
                deleted = False
                for name in names:
                    name = self._to_str(name)
                    if name in self._cache:
                        del self._cache[name]
                        deleted = True
                return deleted
        return self._execute_command(_delete, *names)

    #### LIST COMMANDS ####

    def llen(self, name):
        def _llen(name):
            with self._lock.reader():
                name = self._to_str(name)
                value = self._assert_list(self._cache.get(name, None))
                if value is None:
                    return 0
                return len(value)
        return self._execute_command(_llen, name)

    def lpush(self, name, value):
        def _lpush(name, value):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._to_str(value)
                val = self._assert_list(self._cache.get(name, None))
                val.insert(0, value)
                self._cache[name] = val
                return len(val)
        return self._execute_command(_lpush, name, value)

    def rpush(self, name, value):
        def _rpush(name, value):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._to_str(value)
                val = self._assert_list(self._cache.get(name, None))
                val.append(value)
                self._cache[name] = val
                return len(val)
        return self._execute_command(_rpush, name, value)

    def _lrange(self, name, start, end):
        name = self._to_str(name)
        val = self._assert_list(self._cache.get(name, None))
        end += 1
        if end == 0:
            end = None
        return val[start:end]

    def lrange(self, name, start, end):
        def __lrange(name, start, end):
            with self._lock.writer():
                name = self._to_str(name)
                return self._lrange(name, start, end)
        return self._execute_command(__lrange, name, start, end)

    def ltrim(self, name, start, end):
        def _ltrim(name, start, end):
            with self._lock.writer():
                name = self._to_str(name)

                if name not in self._cache:
                    # name が存在しない場合は何もしない
                    return True

                val = self._lrange(name, start, end)

                if val:
                    self._cache[name] = val
                else:
                    del self._cache[name]
                return True
        return self._execute_command(_ltrim, name, start, end)

    def lrem(self, name, value, num=0):
        """
        Removes the first count occurrences of elements equal to value from the
        list stored at key. The count argument influences the operation in the
        following ways:

        count > 0: Remove elements equal to value moving from head to tail.
        count < 0: Remove elements equal to value moving from tail to head.
        count = 0: Remove all elements equal to value.

        For example, LREM list -2 "hello" will remove the last two occurrences
        of "hello" in the list stored at list.

        Note that non-existing keys are treated like empty lists, so when key
        does not exist, the command will always return 0.
        """
        def _lrem(name, value, num):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._to_str(value)

                if name not in self._cache:
                    # Non-existing keys are treated like empty lists,
                    # so when key does not exist, the command will always return 0.
                    return 0

                val = self._assert_list(self._cache.get(name, None))

                _num = num if num != 0 else None
                if num is not None and num < 0:
                    val = reversed(val)
                    _num *= -1

                new_val = []
                rem_count = 0
                for x in val:
                    if x == value and (_num is None or _num > 0):
                        if _num is not None:
                            _num -= 1
                        rem_count += 1
                    else:
                        new_val.append(x)

                if num < 0:
                    new_val.reverse()

                if new_val:
                    self._cache[name] = new_val
                else:
                    del self._cache[name]
                return rem_count

        return self._execute_command(_lrem, name, value, num)

    #### HASH COMMANDS ####

    def hdel(self, name, *keys):
        def _hdel(name, *keys):
            with self._lock.writer():
                # Emulate Redis < 2.4 for now
                # TODO: Behavior based on _server_verison
                name = self._to_str(name)
                if len(keys) != 1:
                    # When no keys are passed emulate an error
                    # returned from the server.
                    raise ResponseError("wrong number of arguments for 'hdel' command")
                val = self._assert_dict(self._cache.get(name, None))

                deleted_count = 0
                for k in keys:
                    k = self._to_str(k)
                    if k in val:
                        deleted_count+=1
                        del val[k]

                # Emulate Redis < 2.4 for now
                return deleted_count > 0

        return self._execute_command(_hdel, name, *keys)

    def hexists(self, name, key):
        def _hexists(name, keys):
            with self._lock.writer():
                name = self._to_str(name)
                keys = self._to_str(keys)

                val = self._assert_dict(self._cache.get(name, None))
                return keys in val
        return self._execute_command(_hexists, name, key)

    def hget(self, name, key):
        def _hget(name, key):
            with self._lock.writer():
                name = self._to_str(name)
                key = self._to_str(key)

                return self._assert_dict(self._cache.get(name, None)).get(key)
        return self._execute_command(_hget, name, key)

    def hgetall(self, name):
        def _hgetall(name):
            with self._lock.writer():
                # Redis only stores strings in hashes
                # which are immutable in Python so a shallow copy is adequate.
                name = self._to_str(name)
                return self._assert_dict(self._cache.get(name, None)).copy()
        return self._execute_command(_hgetall, name)

    def hset(self, name, key, value):
        def _hset(name, key, value):
            with self._lock.writer():

                name = self._to_str(name)
                key = self._to_str(key)
                value = self._to_str(value)

                val = self._assert_dict(self._cache.get(name, None))
                rtn_val = 0 if key in val else 1
                val[key] = value
                self._cache[name] = val
                return rtn_val

        return self._execute_command(_hset, name, key, value)

    def hlen(self, name):
        def _hlen(name):
            with self._lock.writer():
                name = self._to_str(name)
                return len(self._assert_dict(self._cache.get(name, None)))
        return self._execute_command(_hlen, name)

    #### SET COMMANDS ####

    def sadd(self, name, value):
        def _sadd(name, value):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._to_str(value)

                val = self._assert_set(self._cache.get(name, None))
                if value in val:
                    return False
                val.add(value)
                self._cache[name] = val
                return True

        return self._execute_command(_sadd, name, value)

    def scard(self, name):
        def _scard(name):
            with self._lock.reader():
                name = self._to_str(name)
                val = self._assert_set(self._cache.get(name, None))
                return len(val)
        return self._execute_command(_scard, name)

    def srem(self, name, value):
        def _srem(name, value):
            with self._lock.writer():
                name = self._to_str(name)
                value = self._to_str(value)
                val = self._assert_set(self._cache.get(name, None))
                if value in val:
                    val.remove(value)
                    return True
                else:
                    return False
        return self._execute_command(_srem, name, value)

    def sinter(self, keys, *args):
        def _sinter(keys, *args):
            with self._lock.writer():
                keys = list_or_args(keys, args)
                sets = [self._assert_set(self._cache.get(self._to_str(key), None)) for key in keys]

                if sets:
                    i = sets[0]
                    for s in sets[1:]:
                        i = i.intersection(s)
                    return i
                else:
                    return set()
        return self._execute_command(_sinter, keys, *args)

    def sismember(self, name, value):
        def _sismember(name, value):
            with self._lock.reader():
                name = self._to_str(name)
                value = self._to_str(value)
                val = self._assert_set(self._cache.get(name, None))
                return value in val
        return self._execute_command(_sismember, name, value)

    def smembers(self, name):
        def _smembers(name):
            with self._lock.reader():
                name = self._to_str(name)
                return self._assert_set(self._cache.get(name, None))
        return self._execute_command(_smembers, name)

    #### SERVER COMMANDS ####

    def flushdb(self):
        def _flushdb():
            with self._lock.writer():
                self._cache.clear()
        return self._execute_command(_flushdb)

    def flushall(self):
        def _flushall():
            with self._lock.writer():
                global _caches

                for name in _caches.keys():
                    if name.startswith('%s:%s' % (self._host, self._port)):
                        _caches[name].clear()

        return self._execute_command(_flushall)

    def pipeline(self, transaction=True, shard_hint=None):
        # TODO: Support response_callbacks
        pipe = Pipeline(self._name, self.connection_pool, None, transaction, shard_hint)
        pipe._charset = self._charset
        pipe._errors = self._errors
        return pipe

    def execute_command(self, *args, **options):
        raise NotImplemented("Executing commands is not supported by this Mock")

    def _execute_command(self, cmd, *args, **kwargs):
        return cmd(*args, **kwargs)

    def _assert_int(self, val):
        if val is None:
            return 0

        try:
            val = int(val)
        except ValueError:
            raise ResponseError("ResponseError: value is not an integer or out of range")
        else:
            return val

    def _assert_list(self, val):
        if val is None:
            return []
        if isinstance(val, (list, tuple)):
            return val
        else:
            raise ResponseError("Operation against a key holding the wrong kind of value")

    def _assert_set(self, val):
        if val is None:
            return set()
        if isinstance(val, set):
            return val
        else:
            raise ResponseError("Operation against a key holding the wrong kind of value")

    def _assert_dict(self, val):
        if val is None:
            return {}
        if isinstance(val, dict):
            return val
        else:
            raise ResponseError("Operation against a key holding the wrong kind of value")

    def _assert_str(self, val):
        if val is None:
            return None
        if isinstance(val, str):
            return val
        elif isinstance(val, bytes):
            return val
        else:
            raise ResponseError("Operation against a key holding the wrong kind of value")

    def _to_str(self, value):
        """
        Encodes the unicode objects to str objects based
        on the given encoding.

        Needed to make sure that UnicodeDecodeErrors are thrown
        when saving values.
        """
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode(self._charset, self._errors)
        return bytes(str(value), 'utf-8')

class Pipeline(Redis):
    def __init__(self, name, connection_pool, response_callbacks, transaction, shard_hint):
        self._name = name
        global _caches, _locks
        self._cache = _caches.setdefault(self._name, {})
        self._lock = _locks.setdefault(self._name, RWLock())

        self.connection = None
        self.connection_pool = connection_pool
        self.watching = False
        self.explicit_transaction = False
        self.command_stack = []

        # This is a mock so these are not used
        self.transaction = transaction
        self.shard_hint = shard_hint

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def watch(self, name):
        if self.explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')
        self.watching = True

    def multi(self):
        if self.explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise RedisError('Commands without an initial WATCH have already '
                             'been issued')
        self.explicit_transaction = False

    def execute(self):
        ret_vals = []
        for cmd, args, kwargs in self.command_stack:
            try:
                ret_vals.append(cmd(*args, **kwargs))
            except RedisError as error:
                ret_vals.append(error)
        self.reset()
        return ret_vals

    def reset(self):
        self.watching = False
        self.explicit_transaction = False
        self.command_stack = []

    def _execute_command(self, cmd, *args, **kwargs):
        self.command_stack.append((cmd, args, kwargs))
        return self
