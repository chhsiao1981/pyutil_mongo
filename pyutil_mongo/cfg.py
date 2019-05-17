# -*- coding: utf-8 -*-
"""Summary

Attributes:
    CONFIG (dict): Description
    LOGGER (logging.Logger): Description
    MONGO_MAPS (list[MongoMap]): Description
"""
import logging
from pymongo import MongoClient

from . import errors

CONFIG = {}

MONGO_MAPS = None

LOGGER = None


class MongoMap(object):
    """Summary

    Attributes:
        collection_map (dict): mapping for the collection-name in the code vs. real collection-name in the mongo.
        db (str): db name in the real mongo.
        ensure_index (TYPE): Description
        ensure_unique_index (TYPE): Description
        hostname (str): hostname of the real mongo.
        name (str): the name of the MongoMap.
    """

    collection_map = None
    ensure_index = None
    ensure_unique_index = None
    name = ""
    hostname = ""
    db = ""

    def __init__(self, collection_map: dict, ensure_index=None, ensure_unique_index=None, name="mongo", hostname="localhost:27017", db="test"):
        """Summary

        Args:
            collection_map (dict): Description
            ensure_index (None, optional): Description
            ensure_unique_index (None, optional): Description
            name (str, optional): Description
            hostname (str, optional): Description
            db (str, optional): Description
        """
        self.name = name
        self.hostname = hostname
        self.db = db
        self.collection_map = collection_map
        self.ensure_index = ensure_index
        self.ensure_unique_index = ensure_unique_index


def init(logger: logging.Logger, mongo_maps: list):
    """Summary

    Args:
        logger (logging.Logger): Description
        mongo_maps (list[MongoMap]): Description
    """
    global LOGGER
    global MONGO_MAPS

    LOGGER = logger
    MONGO_MAPS = mongo_maps

    return restart_mongo()


def restart_mongo(collection_name=""):
    """Summary

    Args:
        collection_name (str, optional): Description

    Returns:
        TYPE: Description
    """
    global MONGO_MAPS
    '''
    initialize mongo
    '''

    if MONGO_MAPS is None:
        return errors.DBInvalidMongoMap()

    for mongo_map in MONGO_MAPS:
        err = _init_mongo_map_core(mongo_map, collection_name=collection_name)
        if err is not None:
            return err


def _init_mongo_map_core(mongo_map: MongoMap, collection_name=""):
    """Summary

    Args:
        mongo_map (MongoMap): Description
        collection_name (str, optional): Description

    Returns:
        TYPE: Description

    Deleted Parameters:
        db_name (str, optional): Description
    """
    name, hostname, db, collection_map, ensure_index, ensure_unique_index = mongo_map.name, mongo_map.hostname, mongo_map.db, mongo_map.collection_map, mongo_map.ensure_index, mongo_map.ensure_unique_index

    if collection_name != "" and collection_name not in collection_map:
        return None

    if ensure_index is None:
        ensure_index = {}

    if ensure_unique_index is None:
        ensure_unique_index = {}

    mongo_server_url = name + '_MONGO_SERVER_URL'
    mongo_server_idx = name + '_MONGO_SERVER'

    # mongo_server_url
    if mongo_server_url not in CONFIG:
        CONFIG[mongo_server_url] = "mongodb://" + hostname + "/" + db

    # mongo-server-idx
    if mongo_server_idx not in CONFIG:
        try:
            CONFIG[mongo_server_idx] = MongoClient(
                CONFIG.get(mongo_server_url))[db]
        except Exception as e:
            LOGGER.error(
                'unable to init mongo-client: name: %s hostname: %s db: %s e: %s', name, hostname, db, e)
            return e

    # collection
    for (key, val) in collection_map.items():
        if key in CONFIG and collection_name == "":
            LOGGER.warning(
                'key already in config: key: %s config: %s', key, CONFIG[key])
            continue

        try:
            LOGGER.info('mongo: %s => %s', key, val)
            CONFIG[key] = CONFIG.get(mongo_server_idx)[val]
        except Exception as e:
            LOGGER.error('unable to init mongo: name: %s mongo_server_hostname: %s db: %s e: %s',
                         name, mongo_server_hostname, db, e)

            for key, val in mongo_map.items():
                CONFIG[key] = None
            return e

    # enure index
    for key, val in ensure_index.items():
        LOGGER.info('to ensure_index: key: %s', key)
        try:
            CONFIG[key].create_index(val, background=True)
        except Exception as e:
            LOGGER.error('unable to ensure_index: key: %s e: %s', key, e)
            return e

    # enure unique index
    for key, val in ensure_unique_index.items():
        LOGGER.info('to ensure_unique_index: key: %s', key)
        try:
            CONFIG[key].create_index(val, background=True, unique=True)
        except Exception as e:
            LOGGER.error(
                'unable to ensure unique index: key: %s e: %s', key, e)
            isinstance(e, )
            return e

    return None

def clean():
    global CONFIG
    global MONGO_MAPS

    CONFIG = {}
    MONGO_MAPS = None
