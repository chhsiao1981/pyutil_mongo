# -*- coding: utf-8 -*-

import unittest
import logging
import pymongo
from pymongo.cursor import Cursor

from pyutil_mongo import cfg
from pyutil_mongo import util
from pyutil_mongo import errors


class TestUtil(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger('test')
        collection_map = {
            'a': 'b',
            'a2': 'b',
        }
        ensure_index = {
            'a': [('key1', pymongo.ASCENDING)],
        }
        mongo_map = cfg.MongoMap(collection_map, ensure_index=ensure_index)

        err = cfg.init(self.logger, [mongo_map])

    def tearDown(self):
        util.drop('a')
        cfg.clean()

    def test_db_find_one(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)
        err, db_result = util.db_update('a', {'key1': 'a'}, {'key2': 'b'})
        self.assertIsNone(err)

        err, db_result = util.db_find_one('a', {'key1': 'a'})
        self.assertIsNone(err)
        self.assertEqual(db_result, {'key1': 'a', 'key2': 'b'})

        err, db_result = util.db_find_one('a', {'key1': 'b'})
        self.assertIsNone(err)
        self.assertEqual(db_result, {})

    def test_db_find(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)
        err, db_result = util.db_update('a', {'key1': 'a'}, {'key2': 'b'})
        self.assertIsNone(err)

        err, db_result = util.db_find('a', {'key1': 'a'})
        self.assertIsNone(err)
        self.assertEqual(db_result, [{'key1': 'a', 'key2': 'b'}])

        err, db_result = util.db_find('a', {'key1': 'b'})
        self.assertIsNone(err)
        self.assertEqual(db_result, [])

    def test_db_find_it(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)
        err, db_result = util.db_update('a', {'key1': 'a'}, {'key2': 'b'})
        self.assertIsNone(err)

        err, db_result = util.db_find_it('a', {'key1': 'a'})
        self.assertIsNone(err)
        self.assertIsInstance(db_result, Cursor)

        err, db_result = util.db_find_it('a', {'key1': 'b'})
        self.assertIsNone(err)
        self.assertIsInstance(db_result, Cursor)

    def test_db_insert(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'b'}])
        self.assertIsNone(err)

        err, db_results = util.db_find('a', {'key1': 'a'})
        self.logger.debug('test_db_insert: after db_find: db_results: %s', db_results)
        self.assertIsNone(err)
        self.assertEqual(3, len(db_results))

        err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'b'}])
        self.assertIsNone(err)

        err, db_results = util.db_find('a', {'key1': 'a'})
        self.logger.debug('test_db_insert: after db_find: db_results: %s', db_results)
        self.assertIsNone(err)
        self.assertEqual(6, len(db_results))

    def test_db_insert_one(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_insert_one('a', {'key1': 'a', 'key2': 'b'})
        self.assertIsNone(err)

        err, db_results = util.db_find('a', {'key1': 'a'})
        self.logger.debug('test_db_insert: after db_find: db_results: %s', db_results)
        self.assertIsNone(err)
        self.assertEqual(1, len(db_results))

        err, db_result = util.db_insert_one('a', {'key1': 'a', 'key2': 'b'})
        self.assertIsNone(err)

        err, db_results = util.db_find('a', {'key1': 'a'})
        self.logger.debug('test_db_insert: after db_find: db_results: %s', db_results)
        self.assertIsNone(err)
        self.assertEqual(2, len(db_results))

    def test_db_bulk_update(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_remove('a', {'key1': 'b'})
        self.assertIsNone(err)

        err, db_result = util.db_bulk_update('a', [{'key': {'key1': 'a'}, 'val': {'key2': 'b'}}, {'key': {'key1': 'a'}, 'val': {'key2': 'b'}}, {'key': {'key1': 'a'}, 'val': {'key2': 'c'}}])
        self.assertIsNone(err)

        err, db_results = util.db_find('a', {'key1': 'a'})
        self.logger.debug('test_db_bulk_update: after db_find: db_results: %s', db_results)
        self.assertIsNone(err)
        self.assertEqual(1, len(db_results))
        self.assertEqual([{'key1': 'a', 'key2': 'c'}], db_results)

    def test_db_distinct(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'b'}, {'key1': 'a', 'key2': 'c'}])
        self.assertIsNone(err)

        err, db_results = util.db_distinct('a', 'key2', {'key1': 'a'})
        self.logger.debug('test_db_distinct: after db_distinct: e: %s db_results: %s', err, db_results)
        self.assertIsNone(err)
        self.assertEqual(2, len(db_results))
        self.assertEqual(['b', 'c'], db_results)

    def test_db_set_if_not_exists(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_set_if_not_exists('a', {'key1': 'a'}, {'key2': 'b'})
        self.logger.debug('test_db_set_if_not_exists (1): after db_set_if_not_exists: e: %s db_result: %s', err, db_result)
        self.assertIsNone(err)

        err, db_result = util.db_set_if_not_exists('a', {'key1': 'a'}, {'key2': 'c'})
        self.logger.debug('test_db_set_if_not_exists (2): after db_set_if_not_exists: e: %s db_result: %s', err, db_result)
        self.assertIsNotNone(err)

        err, db_result = util.db_find_one('a', {'key1': 'a'})
        self.assertIsNone(err)
        self.assertEqual({'key1': 'a', 'key2': 'b'}, db_result)

    def test_db_aggregate(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 1}, {'key1': 'a', 'key2': 2}, {'key1': 'a', 'key2': 3}])
        self.assertIsNone(err)

        pipe = [{'$match': {'key1': 'a'}}, {'$group': {'_id': {'key1': '$key1'}, 'key2': {'$sum': '$key2'}}}]
        err, db_results = util.db_aggregate('a', pipe)
        self.logger.debug('test_db_aggregate: e: %s db_results: %s', err, db_results)
        self.assertIsNone(err)
        self.assertEqual([{'_id': {'key1': 'a'}, 'key2': 6}], db_results)

        err, db_results = util.db_aggregate_parse_results(db_results)
        self.assertEqual([{'key1': 'a', 'key2': 6}], db_results)

    def test_db_max(self):
        err, db_result = util.db_remove('a', {'key1': 'a'})
        self.assertIsNone(err)

        err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 1}, {'key1': 'a', 'key2': 2}, {'key1': 'a', 'key2': 3}])
        self.assertIsNone(err)

        err, db_result = util.db_max('a', 'key2', {'key1': 'a'})
        self.assertIsNone(err)
        self.assertEqual(3, db_result)

    def test__flatten_results_with_error(self):
        results_with_error = [(errors.DBInvalidMongoMap(), {}), (errors.DBException(), {})]

        err, results = util._flatten_results_with_error(results_with_error)
        self.logger.debug('after _flatten_results_with_error: e: %s results: %s', err, results)
        self.assertIsNotNone(err)
