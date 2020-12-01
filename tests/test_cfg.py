# -*- coding: utf-8 -*-

import unittest
import logging
import pymongo

from pyutil_mongo import cfg
from pyutil_mongo import util

import mongomock


class TestCfg(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        util.drop('a')
        cfg.clean()

    @mongomock.patch(servers=(('localhost', 27017),))
    def test_init(self):
        logger = logging.getLogger("test")

        collection_map = {
            'a': 'b',
            'a2': 'b',
        }
        ensure_index = {
            'a': [('key1', pymongo.ASCENDING)],
        }
        ensure_unique_index = {
            'a2': [('key2', pymongo.ASCENDING)],
        }
        mongo_map = cfg.MongoMap(collection_map, ensure_index=ensure_index, ensure_unique_index=ensure_unique_index)

        err = cfg.init(logger, [mongo_map])

        logger.debug("cfg.config: %s err: %s", cfg.config, err)

        self.assertIsNone(err)

        self.assertEqual(1, len(cfg.config))
        self.assertEqual(True, 'a' in cfg.config['mongo']['db'])

        err = cfg.init(logger, [mongo_map])
        self.assertIsNotNone(err)
