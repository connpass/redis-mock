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

class RedisMockStringTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache['test-key'] = "スパム"
        self.mock._cache['test-key2'] = "エッグ"
        self.mock._cache['int-val'] = "11"

    def test_get(self):
        val = self.mock.get('test-key')
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "スパム")

    def test_getset(self):
        val = self.mock.getset('test-key', "new-value")
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "スパム")
        self.assertEquals(self.mock._cache['test-key'], "new-value")

    def test_incr(self):
        val = self.mock.incr('int-val')
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "12")
        self.assertEquals(self.mock._cache['int-val'], "12")

    def test_incr_amount(self):
        val = self.mock.incr('int-val', amount=5)
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "16")
        self.assertEquals(self.mock._cache['int-val'], "16")

    def test_new_incr(self):
        val = self.mock.incr('new-int-val')
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "1")
        self.assertEquals(self.mock._cache['new-int-val'], "1")

    def test_new_incr_amount(self):
        val = self.mock.incr('new-int-val', amount=4)
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "4")
        self.assertEquals(self.mock._cache['new-int-val'], "4")

    def test_set(self):
        self.assertTrue(self.mock.set('test-key', "testvalue"))
        self.assertEquals(self.mock._cache['test-key'], "testvalue")

        self.assertTrue(self.mock.set('new-key', "some-new-testvalue"))
        self.assertEquals(self.mock._cache['new-key'], "some-new-testvalue")

    def test_setnx(self):
        self.assertFalse(self.mock.setnx('test-key', "testvalue"))
        self.assertEquals(self.mock._cache['test-key'], "スパム")

        self.assertTrue(self.mock.setnx('new-key', "some-new-testvalue"))
        self.assertEquals(self.mock._cache['new-key'], "some-new-testvalue")
        self.assertFalse(self.mock.setnx('new-key', "some-new-value"))
        self.assertEquals(self.mock._cache['new-key'], "some-new-testvalue")

    def test_set_unicode_value(self):
        self.assertTrue(self.mock.set('test-key', "ほげ"))
        self.assertEquals(self.mock._cache['test-key'], "ほげ")

        self.assertTrue(self.mock.set('new-key', "ほげほげ"))
        self.assertEquals(self.mock._cache['new-key'], "ほげほげ")

    def test_delete(self):
        self.assertTrue(self.mock.delete('test-key'))
        self.assertEquals(self.mock.get('test-key'), None)

    def test_delete_multi(self):
        self.assertTrue(self.mock.delete('test-key', 'test-key2'))
        self.assertEquals(self.mock.get('test-key'), None)
        self.assertEquals(self.mock.get('test-key2'), None)

    def test_delete_noexist(self):
        self.assertFalse(self.mock.delete('test-noexist'))


class RedisMockListTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache['test-key'] = "スパム"
        self.mock._cache['test-list'] = ["スパム", "エッグ"]
        self.mock._cache['test-int-list'] = ['1','2','3','4','5','6','7','8']
        self.mock._cache['test-dup-list'] = ['1','4','3','4','7','4','7','8']

    def test_get_list(self):
        """
        文字列として、リストを取得しようとする場合、
        エラーが発生する
        """
        self.assertRaises(redis.ResponseError,
            self.mock.get, 'test-list')

    def test_set_list(self):
        val = self.mock.set('test-set', [1, 2, 3])
        val = self.mock.get('test-set')
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, str([1, 2, 3]))

    def test_delete(self):
        self.assertTrue(self.mock.delete('test-list'))
        self.assertEquals(self.mock.lrange('test-list', 0, -1), [])

    def test_delete_multi(self):
        self.assertTrue(self.mock.delete('test-key', 'test-list'))
        self.assertEquals(self.mock.get('test-key'), None)
        self.assertEquals(self.mock.get('test-list'), None)
        self.assertEquals(self.mock.lrange('test-list', 0, -1), [])

    def test_llen(self):
        self.assertEquals(self.mock.llen('test-int-list'), 8)

    def test_llen_not_exists(self):
        self.assertEquals(self.mock.llen('test-no-exists'), 0)

    def test_llen_str(self):
        self.assertRaises(redis.ResponseError,
            self.mock.llen, 'test-key')

    def test_lpush(self):
        self.assertEquals(self.mock.lpush('test-int-list', 10), 9)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['10', '1','2','3','4','5','6','7','8'])

    def test_lpush_unicode_value(self):
        self.assertEquals(self.mock.lpush('test-int-list', "ほげ"), 9)
        self.assertEquals(self.mock._cache['test-int-list'], ["ほげ", '1','2','3','4','5','6','7','8'])

    def test_lpush_not_exists(self):
        self.assertEquals(self.mock.lpush('test-not-exists', 10), 1)
        self.assertEquals(self.mock.lrange('test-not-exists', 0, -1), ['10'])
        self.assertEquals(self.mock.lpush('test-not-exists', 11), 2)
        self.assertEquals(self.mock.lrange('test-not-exists', 0, -1), ['11', '10'])

    def test_lpush_str(self):
        self.assertRaises(redis.ResponseError,
            self.mock.lpush, 'test-key', 10)

    def test_rpush(self):
        self.assertEquals(self.mock.rpush('test-int-list', 10), 9)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['1','2','3','4','5','6','7','8', '10'])

    def test_rpush_unicode_value(self):
        self.assertEquals(self.mock.rpush('test-int-list', "ほげ"), 9)
        self.assertEquals(self.mock._cache['test-int-list'], ['1','2','3','4','5','6','7','8', "ほげ"])

    def test_lrange_all(self):
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['1','2','3','4','5','6','7','8'])

    def test_lrange_index_zero_start(self):
        self.assertEquals(self.mock.lrange('test-int-list', 0, 2), ['1','2','3'])

    def test_lrange_index_nonzero_start(self):
        self.assertEquals(self.mock.lrange('test-int-list', 2, 5), ['3','4','5', '6'])

    def test_lrange_neg_index(self):
        self.assertEquals(self.mock.lrange('test-int-list', 0, -2), ['1','2','3','4','5','6','7'])
        self.assertEquals(self.mock.lrange('test-int-list', 2, -4), ['3','4','5'])

    def test_ltrim_out_of_bounds(self):
        """
        LTRIM trimming using indexes that are out of bounds.
        """
        self.mock.ltrim('test-int-list', 10, 20)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), [])  # Empty
        self.assertEquals(self.mock.get('test-int-list'), None)

    def test_ltrim_neg_index(self):
        self.assertEquals(self.mock.ltrim('test-int-list', 0, -2), True)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['1','2','3','4','5','6','7'])

    def test_ltrim_neg_index2(self):
        self.assertEquals(self.mock.ltrim('test-int-list', 2, -4), True)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['3','4','5'])

    def test_ltrim_non_exist(self):
        self.assertEquals(self.mock.ltrim('test-non-exist', 2, -4), True)
        self.assertEquals(self.mock.lrange('test-non-exist', 0, -1), [])
        self.assertEquals(self.mock.get('test-non-exist'), None)

    def test_ltrim_normal(self):
        self.assertEquals(self.mock.ltrim('test-int-list', 0, 3), True)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['1', '2', '3', '4']) # ['1', '2', '3'] ではない

    def test_ltrim_normal2(self):
        self.assertEquals(self.mock.ltrim('test-int-list', 2, 5), True)
        self.assertEquals(self.mock.lrange('test-int-list', 0, -1), ['3', '4', '5', '6'])

    def test_lrem_normal(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, 1), 1)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','3','4','7','4','7','8'])

    def test_lrem_multi(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, 2), 2)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','3','7','4','7','8'])

    def test_lrem_over(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, 4), 3)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','3','7','7','8'])

    def test_lrem_neg_single(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, -1), 1)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','4','3','4','7','7','8'])

    def test_lrem_neg_multi(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, -2), 2)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','4','3','7','7','8'])

    def test_lrem_neg_over(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, -4), 3)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','3','7','7','8'])

    def test_lrem_all(self):
        self.assertEquals(self.mock.lrem('test-dup-list', 4, 0), 3)
        self.assertEquals(self.mock.lrange('test-dup-list', 0, -1), ['1','3','7','7','8'])

class RedisMockSetTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache['test-key'] = "スパム"
        self.mock._cache['test-string-set'] = set([
            "スパム",
            "エッグ",
        ])
        self.mock._cache['test-int-set'] = set(['1','2','3','4','5','6','7','8'])

    def test_sadd_existing(self):
        self.assertFalse(self.mock.sadd('test-int-set', 5))
        self.assertEqual(self.mock._cache['test-int-set'], set(['1','2','3','4','5','6','7','8']))

    def test_sadd_new(self):
        self.assertTrue(self.mock.sadd('test-int-set', 9))
        self.assertEqual(self.mock._cache['test-int-set'], set(['1','2','3','4','5','6','7','8', '9']))

    def test_sadd_unicode_value(self):
        self.assertTrue(self.mock.sadd('test-string-set', "ほげ"))
        self.assertEquals(self.mock._cache['test-string-set'], set([
            "スパム",
            "エッグ",
            "ほげ",
        ]))

    def test_srem(self):
        self.assertTrue(self.mock.srem('test-int-set', 8))
        self.assertEqual(self.mock._cache['test-int-set'], set(['1','2','3','4','5','6','7']))
        self.assertTrue(self.mock.srem('test-int-set', 1))
        self.assertEqual(self.mock._cache['test-int-set'], set(['2','3','4','5','6','7']))

    def test_srem_unicode_value(self):
        self.assertTrue(self.mock.srem('test-string-set', "スパム"))
        self.assertEquals(self.mock._cache['test-string-set'], set([
            "エッグ",
        ]))

    def test_srem_notexists(self):
        self.assertFalse(self.mock.srem('test-nonexists', 100))

    def test_sismember_number(self):
        self.assertTrue(self.mock.sismember('test-int-set', 5))
        self.assertTrue(self.mock.sismember('test-int-set', '4'))
        self.assertFalse(self.mock.sismember('test-int-set', 0))
        self.assertFalse(self.mock.sismember('test-int-set', '9'))

    def test_sismember_unicode_value(self):
        self.assertTrue(self.mock.sismember('test-string-set', "スパム"))
        self.assertFalse(self.mock.sismember('test-string-set', "ほげほげ"))

    def test_sismember_notexists(self):
        self.assertFalse(self.mock.sismember('test-nonexists', "spam"))

class RedisMockHashTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache['test-key'] = "スパム"
        self.mock._cache['test-hash'] = {
            "hashkey1": "スパム",
            "hashkey2": "エッグ",
        }

    def test_hget(self):
        val = self.mock.hget('test-hash', "hashkey1")
        self.assertTrue(isinstance(val, str))
        self.assertEquals(val, "スパム")

    def test_hget_none(self):
        val = self.mock.hget('test-hash', "not-exists")
        self.assertTrue(val is None)

        val = self.mock.hget('not-exists', "not-exists")
        self.assertTrue(val is None)

    def test_hget_bad_val(self):
        self.assertRaises(redis.ResponseError,
            self.mock.hget, 'test-key', "not-exists")

    def test_hgetall(self):
        val = self.mock.hgetall('test-hash')
        self.assertTrue(isinstance(val, dict))
        self.assertEquals(val, self.mock._cache['test-hash'])

    def test_hgetall_none(self):
        val = self.mock.hgetall('not-exists')
        self.assertTrue(isinstance(val, dict))
        self.assertEquals(val, {})

    def test_hset_new_key(self):
        val = self.mock.hset('test-hash', 'new-key', 'value')
        self.assertEquals(val, 1)

    def test_hset_new_hash(self):
        val = self.mock.hset('new-hash', 'new-key', 'value')
        self.assertEquals(val, 1)

    def test_hset_update(self):
        val = self.mock.hset('test-hash', 'hashkey1', 'new-value')
        self.assertEquals(val, 0)

    def test_hset_unicode(self):
        val = self.mock.hset('test-hash', "ほげ", "ホゲ")
        self.assertEquals(val, 1)
        self.assertEquals(self.mock._cache['test-hash'].get("ほげ"), "ホゲ")

    def test_hset_bad_val(self):
        self.assertRaises(redis.ResponseError,
            self.mock.hset, 'test-key', 'not-exists', 'some-val')

    def test_hlen(self):
        self.assertEquals(self.mock.hlen('test-hash'), 2)
        self.assertEquals(self.mock.hlen('not-exists'), 0)

    def test_hgetall_bad_val(self):
        self.assertRaises(redis.ResponseError,
            self.mock.hgetall, 'test-key')

    def test_hdel_true(self):
        # Redis < 2.5 servers return True/False
        self.assertTrue(self.mock.hdel('test-hash', "hashkey1"))
        self.assertTrue('hashkey1' not in self.mock._cache['test-hash'])

    def test_hdel_false(self):
        # Redis < 2.4 servers return True/False
        self.assertFalse(self.mock.hdel('test-hash', 'not-exists'))

    def test_hdel_bad_val(self):
        self.assertRaises(redis.ResponseError,
            self.mock.hdel, 'test-key', 'some-val')

    def test_hexists(self):
        self.assertTrue(self.mock.hexists('test-hash', 'hashkey1'))
        self.assertFalse(self.mock.hexists('test-hash', 'not-exists'))
        self.assertFalse(self.mock.hexists('not-exists', 'not-exists'))

    def test_hexists_bad_val(self):
        self.assertRaises(redis.ResponseError,
            self.mock.hexists, 'test-key', 'some-key')

class RedisPipelineTest(TestCase):
    def setUp(self):
        self.mock = redis_mock.Redis()
        self.mock._cache.clear()
        self.mock._cache['test-key'] = "スパム"
        self.mock._cache['test-key2'] = "エッグ"

    def test_simple_pipeline(self):
        pipe = self.mock.pipeline()
        self.assertEquals(pipe, pipe.get("test-key"))
        self.assertEquals(pipe, pipe.get("test-key2"))
        self.assertEquals(pipe, pipe.get("not-exists"))

        value = pipe.execute()
        self.assertEquals(value, ["スパム", "エッグ", None])

    def test_rpush(self):
        pipe = self.mock.pipeline()

        self.assertEquals(pipe, pipe.rpush("test-list", "value1"))
        self.assertEquals(pipe, pipe.rpush("test-list", "value2"))
        self.assertEquals(pipe, pipe.rpush("test-list", "value3"))
        self.assertEquals(pipe, pipe.rpush("test-list", "value4"))

        value = pipe.execute()
        self.assertEquals(value, [1, 2, 3, 4])
