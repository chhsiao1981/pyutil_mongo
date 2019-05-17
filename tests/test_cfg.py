# -*- coding: utf-8 -*-

import unittest
import logging
import pymongo

from pyutil_mongo import cfg
from pyutil_mongo import util


class TestCfg(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        util.drop('a')
        cfg.clean()

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

        logger.debug("cfg.CONFIG: %s err: %s", cfg.CONFIG, err)

        self.assertIsNone(err)

        self.assertEqual(4, len(cfg.CONFIG))
        self.assertEqual(True, 'a' in cfg.CONFIG)

        err = cfg.init(logger, [mongo_map])
        self.assertIsNone(err)
