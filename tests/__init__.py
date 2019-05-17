# -*- coding: utf-8 -*-

import unittest
import logging

import pyutil_mongo
from pyutil_mongo import cfg
from pyutil_mongo import util


class TestInit(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        util.drop('a')
        cfg.clean()

    def test_init(self):
        logger = logging.getLogger("test")

        collection_map = {
            'a': 'b',
        }
        mongo_map = pyutil_mongo.MongoMap(collection_map)

        err = pyutil_mongo.init(logger, [mongo_map])

        logger.debug('cfg.CONFIG: %s', cfg.CONFIG)

        self.assertIsNone(err)

        self.assertEqual(3, len(cfg.CONFIG))
        self.assertEqual(True, 'a' in cfg.CONFIG)
