#:coding=utf-8:

from unittest import TestCase

import redis

import redis_mock

__all__ = (
    'RedisMockStringTest',
    'RedisMockListTest',
    'RedisMockSetTest',
    'RedisMockHashTest',
    'RedisPipelineTest',
)

def str_to_bytes(s):
    return bytes(s, 'utf-8')


class RedisMockInternalsTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-string'] = str_to_bytes("スパム")
        self.mock._cache[b'test-list'] = list(map(str_to_bytes, ["スパム", "エッグ"]))
        self.mock._cache[b'test-hash'] = {
            b"hashkey1": str_to_bytes("スパム"),
            b"hashkey2": str_to_bytes("エッグ"),
        }
        self.mock._cache[b'test-set'] = {b"1", b"2"}
        # zset unsupported
        # stream unsupported

    def test_is_type(self):
        self.assertTrue(self.mock._is_type(self.mock._cache[b'test-string'], 'string'))
        self.assertTrue(self.mock._is_type(self.mock._cache[b'test-list'], 'list'))
        self.assertTrue(self.mock._is_type(self.mock._cache[b'test-hash'], 'hash'))
        self.assertTrue(self.mock._is_type(self.mock._cache[b'test-set'], 'set'))

        with self.assertRaises(NotImplementedError):
            self.mock._is_type(self.mock._cache[b'test-string'], 'zset')
        with self.assertRaises(NotImplementedError):
            self.mock._is_type(self.mock._cache[b'test-string'], 'stream')

    def test_key_match(self):
        self.assertTrue(self.mock._key_match('*', b'abc'))
        self.assertTrue(self.mock._key_match('ab*', b'abc'))
        self.assertTrue(self.mock._key_match('abc*', b'abc'))
        self.assertFalse(self.mock._key_match('abcd*', b'abc'))

class RedisMockBasicsTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-string'] = str_to_bytes("スパム")
        self.mock._cache[b'test-string2'] = str_to_bytes("エッグ")
        self.mock._cache[b'test-list'] = list(map(str_to_bytes, ["スパム", "エッグ"]))
        self.mock._cache[b'test-hash'] = {
            b"hashkey1": str_to_bytes("スパム"),
            b"hashkey2": str_to_bytes("エッグ"),
        }
        self.mock._cache[b'test-set'] = {b"1", b"2"}

    def test_keys(self):
        self.assertEqual(set(self.mock.keys()), {b'test-string', b'test-string2', b'test-list', b'test-hash', b'test-set'})
        self.assertEqual(set(self.mock.keys('test-string*')), {b'test-string', b'test-string2'})

    def test_scan(self):
        # pattern
        self.assertEqual(set(self.mock.scan(0, '*', None, None)), {b'test-string', b'test-string2', b'test-list', b'test-hash', b'test-set'})
        self.assertEqual(set(self.mock.scan(0, 'test-string', None, None)), {b'test-string'})
        self.assertEqual(set(self.mock.scan(0, 'test-string*', None, None)), {b'test-string', b'test-string2'})
        self.assertEqual(set(self.mock.scan(0, 'test-list*', None, None)), {b'test-list'})

        # count limit
        self.assertEqual(set(self.mock.scan(0, 'test-string*', 1, None)), {b'test-string'})
        self.assertEqual(set(self.mock.scan(0, 'test-string*', 2, None)), {b'test-string', b'test-string2'})
        self.assertEqual(set(self.mock.scan(1, 'test-string*', 1, None)), {b'test-string2'})
        self.assertEqual(set(self.mock.scan(0, '*', 1, None)), {b'test-string'})

        # type filter
        self.assertEqual(set(self.mock.scan(0, '*', None, 'hash')), {b'test-hash'})
        self.assertEqual(set(self.mock.scan(0, '*', None, 'string')), {b'test-string', b'test-string2'})

        # type mismatch
        self.assertEqual(set(self.mock.scan(0, 'test-hash*', None, 'set')), set())
        self.assertEqual(set(self.mock.scan(0, 'test-set*', None, 'hash')), set())
        # no key
        self.assertEqual(set(self.mock.scan(0, '-*', 0)), set())

class RedisMockStringTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-key'] = str_to_bytes("スパム")
        self.mock._cache[b'test-key2'] = str_to_bytes("エッグ")
        self.mock._cache[b'int-val'] = str_to_bytes("11")

    def test_get(self):
        val = self.mock.get('test-key')
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("スパム"))

    def test_getset(self):
        val = self.mock.getset('test-key', "new-value")
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("スパム"))
        self.assertEqual(self.mock._cache[b'test-key'], str_to_bytes("new-value"))

    def test_incr(self):
        val = self.mock.incr('int-val')
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("12"))
        self.assertEqual(self.mock._cache[b'int-val'], str_to_bytes("12"))

    def test_incr_amount(self):
        val = self.mock.incr('int-val', amount=5)
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("16"))
        self.assertEqual(self.mock._cache[b'int-val'], str_to_bytes("16"))

    def test_new_incr(self):
        val = self.mock.incr('new-int-val')
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("1"))
        self.assertEqual(self.mock._cache[b'new-int-val'], str_to_bytes("1"))

    def test_new_incr_amount(self):
        val = self.mock.incr('new-int-val', amount=4)
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("4"))
        self.assertEqual(self.mock._cache[b'new-int-val'], str_to_bytes("4"))

    def test_set(self):
        self.assertTrue(self.mock.set('test-key', "testvalue"))
        self.assertEqual(self.mock._cache[b'test-key'], str_to_bytes("testvalue"))

        self.assertTrue(self.mock.set('new-key', "some-new-testvalue"))
        self.assertEqual(self.mock._cache[b'new-key'], str_to_bytes("some-new-testvalue"))

    def test_setnx(self):
        self.assertFalse(self.mock.setnx('test-key', "testvalue"))
        self.assertEqual(self.mock._cache[b'test-key'], str_to_bytes("スパム"))

        self.assertTrue(self.mock.setnx('new-key', "some-new-testvalue"))
        self.assertEqual(self.mock._cache[b'new-key'], str_to_bytes("some-new-testvalue"))
        self.assertFalse(self.mock.setnx('new-key', "some-new-value"))
        self.assertEqual(self.mock._cache[b'new-key'], str_to_bytes("some-new-testvalue"))

    def test_set_unicode_value(self):
        self.assertTrue(self.mock.set('test-key', "ほげ"))
        self.assertEqual(self.mock._cache[b'test-key'], str_to_bytes("ほげ"))

        self.assertTrue(self.mock.set('new-key', "ほげほげ"))
        self.assertEqual(self.mock._cache[b'new-key'], str_to_bytes("ほげほげ"))

    def test_delete(self):
        self.assertTrue(self.mock.delete('test-key'))
        self.assertEqual(self.mock.get('test-key'), None)

    def test_delete_multi(self):
        self.assertTrue(self.mock.delete('test-key', 'test-key2'))
        self.assertEqual(self.mock.get('test-key'), None)
        self.assertEqual(self.mock.get('test-key2'), None)

    def test_delete_noexist(self):
        self.assertFalse(self.mock.delete('test-noexist'))

class RedisMockListTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-key'] = str_to_bytes("スパム")
        self.mock._cache[b'test-list'] = list(map(str_to_bytes, ["スパム", "エッグ"]))
        self.mock._cache[b'test-int-list'] = list(map(str_to_bytes, ['1','2','3','4','5','6','7','8']))
        self.mock._cache[b'test-dup-list'] = list(map(str_to_bytes, ['1','4','3','4','7','4','7','8']))

    def test_get_list(self):
        """
        文字列として、リストを取得しようとする場合、
        エラーが発生する
        """
        self.assertRaises(redis.ResponseError, self.mock.get, 'test-list')

    def test_set_list(self):
        val = self.mock.set('test-set', [1, 2, 3])
        val = self.mock.get('test-set')
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes(str([1, 2, 3])))

    def test_delete(self):
        self.assertTrue(self.mock.delete('test-list'))
        self.assertEqual(self.mock.lrange('test-list', 0, -1), [])

    def test_delete_multi(self):
        self.assertTrue(self.mock.delete('test-key', 'test-list'))
        self.assertEqual(self.mock.get('test-key'), None)
        self.assertEqual(self.mock.get('test-list'), None)
        self.assertEqual(self.mock.lrange('test-list', 0, -1), [])

    def test_llen(self):
        self.assertEqual(self.mock.llen('test-int-list'), 8)

    def test_llen_not_exists(self):
        self.assertEqual(self.mock.llen('test-no-exists'), 0)

    def test_llen_str(self):
        self.assertRaises(redis.ResponseError,
            self.mock.llen, 'test-key')

    def test_lpush(self):
        self.assertEqual(self.mock.lpush('test-int-list', 10), 9)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['10', '1','2','3','4','5','6','7','8'])))

    def test_lpush_unicode_value(self):
        self.assertEqual(self.mock.lpush('test-int-list', "ほげ"), 9)
        self.assertEqual(self.mock._cache[str_to_bytes('test-int-list')], list(map(str_to_bytes, ["ほげ", '1','2','3','4','5','6','7','8'])))

    def test_lpush_not_exists(self):
        self.assertEqual(self.mock.lpush('test-not-exists', 10), 1)
        self.assertEqual(self.mock.lrange('test-not-exists', 0, -1), list(map(str_to_bytes, ['10'])))
        self.assertEqual(self.mock.lpush('test-not-exists', 11), 2)
        self.assertEqual(self.mock.lrange('test-not-exists', 0, -1), list(map(str_to_bytes, ['11', '10'])))

    def test_lpush_str(self):
        self.assertRaises(redis.ResponseError, self.mock.lpush, 'test-key', 10)

    def test_rpush(self):
        self.assertEqual(self.mock.rpush('test-int-list', 10), 9)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['1','2','3','4','5','6','7','8', '10'])))

    def test_rpush_unicode_value(self):
        self.assertEqual(self.mock.rpush('test-int-list', "ほげ"), 9)
        self.assertEqual(self.mock._cache[str_to_bytes('test-int-list')], list(map(str_to_bytes, ['1','2','3','4','5','6','7','8', "ほげ"])))

    def test_lrange_all(self):
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['1','2','3','4','5','6','7','8'])))

    def test_lrange_index_zero_start(self):
        self.assertEqual(self.mock.lrange('test-int-list', 0, 2), list(map(str_to_bytes, ['1','2','3'])))

    def test_lrange_index_nonzero_start(self):
        self.assertEqual(self.mock.lrange('test-int-list', 2, 5), list(map(str_to_bytes, ['3','4','5', '6'])))

    def test_lrange_neg_index(self):
        self.assertEqual(self.mock.lrange('test-int-list', 0, -2), list(map(str_to_bytes, ['1','2','3','4','5','6','7'])))
        self.assertEqual(self.mock.lrange('test-int-list', 2, -4), list(map(str_to_bytes, ['3','4','5'])))

    def test_ltrim_out_of_bounds(self):
        """
        LTRIM trimming using indexes that are out of bounds.
        """
        self.mock.ltrim('test-int-list', 10, 20)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), [])  # Empty
        self.assertEqual(self.mock.get('test-int-list'), None)

    def test_ltrim_neg_index(self):
        self.assertEqual(self.mock.ltrim('test-int-list', 0, -2), True)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['1','2','3','4','5','6','7'])))

    def test_ltrim_neg_index2(self):
        self.assertEqual(self.mock.ltrim('test-int-list', 2, -4), True)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['3','4','5'])))

    def test_ltrim_non_exist(self):
        self.assertEqual(self.mock.ltrim('test-non-exist', 2, -4), True)
        self.assertEqual(self.mock.lrange('test-non-exist', 0, -1), [])
        self.assertEqual(self.mock.get('test-non-exist'), None)

    def test_ltrim_normal(self):
        self.assertEqual(self.mock.ltrim('test-int-list', 0, 3), True)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['1', '2', '3', '4']))) # ['1', '2', '3'] ではない

    def test_ltrim_normal2(self):
        self.assertEqual(self.mock.ltrim('test-int-list', 2, 5), True)
        self.assertEqual(self.mock.lrange('test-int-list', 0, -1), list(map(str_to_bytes, ['3', '4', '5', '6'])))

    def test_lrem_normal(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, 1), 1)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','3','4','7','4','7','8'])))

    def test_lrem_multi(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, 2), 2)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','3','7','4','7','8'])))

    def test_lrem_over(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, 4), 3)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','3','7','7','8'])))

    def test_lrem_neg_single(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, -1), 1)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','4','3','4','7','7','8'])))

    def test_lrem_neg_multi(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, -2), 2)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','4','3','7','7','8'])))

    def test_lrem_neg_over(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, -4), 3)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','3','7','7','8'])))

    def test_lrem_all(self):
        self.assertEqual(self.mock.lrem('test-dup-list', 4, 0), 3)
        self.assertEqual(self.mock.lrange('test-dup-list', 0, -1), list(map(str_to_bytes, ['1','3','7','7','8'])))

class RedisMockSetTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-key'] = "スパム"
        self.mock._cache[b'test-string-set'] = set(map(str_to_bytes, [
            "スパム",
            "エッグ",
        ]))
        self.mock._cache[b'test-int-set'] = set(map(str_to_bytes, ['1','2','3','4','5','6','7','8']))

    def test_sadd_existing(self):
        self.assertFalse(self.mock.sadd('test-int-set', 5))
        self.assertEqual(self.mock._cache[b'test-int-set'], set(map(str_to_bytes, ['1','2','3','4','5','6','7','8'])))

    def test_sadd_new(self):
        self.assertTrue(self.mock.sadd('test-int-set', 9))
        self.assertEqual(self.mock._cache[b'test-int-set'], set(map(str_to_bytes, ['1','2','3','4','5','6','7','8', '9'])))

    def test_sadd_unicode_value(self):
        self.assertTrue(self.mock.sadd('test-string-set', "ほげ"))
        self.assertEqual(self.mock._cache[b'test-string-set'], set(map(str_to_bytes, [
            "スパム",
            "エッグ",
            "ほげ",
        ])))

    def test_srem(self):
        self.assertTrue(self.mock.srem('test-int-set', 8))
        self.assertEqual(self.mock._cache[b'test-int-set'], set(map(str_to_bytes, ['1','2','3','4','5','6','7'])))
        self.assertTrue(self.mock.srem('test-int-set', 1))
        self.assertEqual(self.mock._cache[b'test-int-set'], set(map(str_to_bytes, ['2','3','4','5','6','7'])))

    def test_srem_unicode_value(self):
        self.assertTrue(self.mock.srem('test-string-set', "スパム"))
        self.assertEqual(self.mock._cache[b'test-string-set'], set(map(str_to_bytes, [
            "エッグ",
        ])))

    def test_srem_notexists(self):
        self.assertFalse(self.mock.srem('test-nonexists', 100))

    def test_sismember_number(self):
        self.assertTrue(self.mock.sismember('test-int-set', 5))
        self.assertTrue(self.mock.sismember('test-int-set', str_to_bytes('4')))
        self.assertFalse(self.mock.sismember('test-int-set', 0))
        self.assertFalse(self.mock.sismember('test-int-set', str_to_bytes('9')))

    def test_sismember_unicode_value(self):
        self.assertTrue(self.mock.sismember('test-string-set', str_to_bytes("スパム")))
        self.assertFalse(self.mock.sismember('test-string-set', str_to_bytes("ほげほげ")))

    def test_sismember_notexists(self):
        self.assertFalse(self.mock.sismember('test-nonexists', str_to_bytes("spam")))

class RedisMockHashTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-key'] = str_to_bytes("スパム")
        self.mock._cache[b'test-hash'] = {
            b"hashkey1": str_to_bytes("スパム"),
            b"hashkey2": str_to_bytes("エッグ"),
        }

    def test_hget(self):
        val = self.mock.hget('test-hash', "hashkey1")
        self.assertTrue(isinstance(val, bytes))
        self.assertEqual(val, str_to_bytes("スパム"))

    def test_hget_none(self):
        val = self.mock.hget('test-hash', "not-exists")
        self.assertTrue(val is None)

        val = self.mock.hget('not-exists', "not-exists")
        self.assertTrue(val is None)

    def test_hget_bad_val(self):
        self.assertRaises(redis.ResponseError, self.mock.hget, 'test-key', "not-exists")

    def test_hgetall(self):
        val = self.mock.hgetall('test-hash')
        self.assertTrue(isinstance(val, dict))
        self.assertEqual(val, self.mock._cache[b'test-hash'])

    def test_hgetall_none(self):
        val = self.mock.hgetall('not-exists')
        self.assertTrue(isinstance(val, dict))
        self.assertEqual(val, {})

    def test_hset_new_key(self):
        val = self.mock.hset('test-hash', 'new-key', 'value')
        self.assertEqual(val, 1)

    def test_hset_new_hash(self):
        val = self.mock.hset('new-hash', 'new-key', 'value')
        self.assertEqual(val, 1)

    def test_hset_update(self):
        val = self.mock.hset('test-hash', 'hashkey1', 'new-value')
        self.assertEqual(val, 0)

    def test_hset_unicode(self):
        val = self.mock.hset('test-hash', "ほげ", "ホゲ")
        self.assertEqual(val, 1)
        self.assertEqual(self.mock._cache[b'test-hash'].get(str_to_bytes("ほげ")), str_to_bytes("ホゲ"))

    def test_hset_bad_val(self):
        self.assertRaises(redis.ResponseError, self.mock.hset, 'test-key', 'not-exists', 'some-val')

    def test_hlen(self):
        self.assertEqual(self.mock.hlen('test-hash'), 2)
        self.assertEqual(self.mock.hlen('not-exists'), 0)

    def test_hgetall_bad_val(self):
        self.assertRaises(redis.ResponseError, self.mock.hgetall, 'test-key')

    def test_hdel_true(self):
        # Redis < 2.5 servers return True/False
        self.assertTrue(self.mock.hdel('test-hash', "hashkey1"))
        self.assertTrue(b'hashkey1' not in self.mock._cache[b'test-hash'])

    def test_hdel_false(self):
        # Redis < 2.4 servers return True/False
        self.assertFalse(self.mock.hdel('test-hash', 'not-exists'))

    def test_hdel_bad_val(self):
        self.assertRaises(redis.ResponseError, self.mock.hdel, 'test-key', 'some-val')

    def test_hexists(self):
        self.assertTrue(self.mock.hexists('test-hash', 'hashkey1'))
        self.assertFalse(self.mock.hexists('test-hash', 'not-exists'))
        self.assertFalse(self.mock.hexists('not-exists', 'not-exists'))

    def test_hexists_bad_val(self):
        self.assertRaises(redis.ResponseError, self.mock.hexists, 'test-key', 'some-key')

class RedisPipelineTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache[b'test-key'] = str_to_bytes("スパム")
        self.mock._cache[b'test-key2'] = str_to_bytes("エッグ")

    def test_simple_pipeline(self):
        pipe = self.mock.pipeline()
        self.assertEqual(pipe, pipe.get("test-key"))
        self.assertEqual(pipe, pipe.get("test-key2"))
        self.assertEqual(pipe, pipe.get("not-exists"))

        value = pipe.execute()
        self.assertEqual(value, [str_to_bytes("スパム"), str_to_bytes("エッグ"), None])

    def test_rpush(self):
        pipe = self.mock.pipeline()

        self.assertEqual(pipe, pipe.rpush("test-list", "value1"))
        self.assertEqual(pipe, pipe.rpush("test-list", "value2"))
        self.assertEqual(pipe, pipe.rpush("test-list", "value3"))
        self.assertEqual(pipe, pipe.rpush("test-list", "value4"))

        value = pipe.execute()
        self.assertEqual(value, [1, 2, 3, 4])


class RedisVsMock(TestCase):
    '''Please note that this is a destructive test
    Any data in the specified Redis server database will be destroyed.
    '''
    def setUp(self):
        # use db=10 as it is less likely to be used, still this isn't safe
        self.redis = redis.Redis(db=10)
        self.redis.flushdb()
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()

    def tearDown(self):
        self.redis.flushdb()

    def test_string(self):
        def set(key, value):
            self.redis.set(key, value)
            self.mock.set(key, value)
        def get(key):
            return self.redis.get(key), self.mock.get(key)

        set('test-key', 1)
        self.assertEqual(*get('test-key'))
        set('test-key', 1.23)
        self.assertEqual(*get('test-key'))
        set('test-key', "スパム")
        self.assertEqual(*get('test-key'))
        set('test-key2', "エッグ")
        self.assertEqual(*get('test-key2'))

    def test_list(self):
        def rpush(key, value):
            self.redis.rpush(key, value)
            self.mock.rpush(key, value)
        def lrange(key, start=0, end=-1):
            return (
                self.redis.lrange(key, start, end),
                self.mock.lrange(key, start, end)
            )

        rpush('test-list', 'value1')
        rpush('test-list', 'value2')
        self.assertEqual(*lrange('test-list'))

    def test_hash(self):
        def hset(key, field, value):
            self.redis.hset(key, field, value)
            self.mock.hset(key, field, value)
        def hget(key, field):
            return self.redis.hget(key, field), self.mock.hget(key, field)

        hset('test-hash', 'hashkey1', "スパム")
        hset('test-hash', 'hashkey2', "エッグ")

        self.assertEqual(*hget('test-hash', 'hashkey1'))
        self.assertEqual(*hget('test-hash', 'hashkey2'))

    def test_set(self):
        def sadd(key, values):
            self.redis.sadd(key, values)
            self.mock.sadd(key, values)
        def sadd_multiple(key, values):
            for value in values:
                sadd(key, value)
        def smembers(key):
            return self.redis.smembers(key), self.mock.smembers(key)

        sadd_multiple('test-string-set', {"スパム", "エッグ"})
        sadd_multiple('test-int-set', {1,2,3,4,5,6,7,8})

        self.assertEqual(*smembers('test-string-set'))
        self.assertEqual(*smembers('test-int-set'))
