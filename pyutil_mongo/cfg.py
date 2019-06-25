# -*- coding: utf-8 -*-
"""Summary

Attributes:
    config (dict): Description
    logger (logging.Logger): Description
    MONGO_MAPS (list[MongoMap]): Description
"""
import logging
from pymongo import MongoClient

logger = None
config = {}

MONGO_MAPS = None


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
    ssl = False
    cert = None
    ca = None

    def __init__(self, collection_map: dict, ensure_index=None, ensure_unique_index=None, name="mongo", hostname="localhost:27017", db="test", ssl=False, cert=None, ca=None):
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
        self.ssl = ssl
        self.cert = cert
        self.ca = ca


def init(the_logger: logging.Logger, mongo_maps: list):
    """Summary

    Args:
        logger (logging.Logger): Description
        mongo_maps (list[MongoMap]): Description
    """
    global logger
    global MONGO_MAPS

    logger = the_logger
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
        raise Exception('invalid mongo-map')

    for mongo_map in MONGO_MAPS:
        _init_mongo_map_core(mongo_map, collection_name=collection_name)


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
    global config
    global logger

    name, hostname, db, collection_map, ensure_index, ensure_unique_index = mongo_map.name, mongo_map.hostname, mongo_map.db, mongo_map.collection_map, mongo_map.ensure_index, mongo_map.ensure_unique_index

    if collection_name != '' and collection_name not in collection_map:
        return

    if ensure_index is None:
        ensure_index = {}

    if ensure_unique_index is None:
        ensure_unique_index = {}

    mongo_server_url = name + '_MONGO_SERVER_URL'
    mongo_server_idx = name + '_MONGO_SERVER'

    # mongo_server_url
    if mongo_server_url not in config:
        config[mongo_server_url] = "mongodb://" + hostname + "/" + db

    # mongo-server-idx
    if mongo_server_idx not in config:
        mongo_kwargs = {}
        if mongo_map.ssl:
            mongo_kwargs.update({
                'ssl': True,
                'authSource': '$external',
                'authMechanism': 'MONGODB-X509',
                'ssl_certfile': mongo_map.cert,
                'ssl_ca_certs': mongo_map.ca,
            })

        config[mongo_server_idx] = MongoClient(
            config.get(mongo_server_url),
            **mongo_kwargs,
        )[db]

    # collection
    for (key, val) in collection_map.items():
        if key in config and collection_name == "":
            logger.warning('key already in config: key: %s config: %s', key, config[key])
            continue

        logger.info('mongo: %s => %s', key, val)
        config[key] = config.get(mongo_server_idx)[val]

    # enure index
    for key, val in ensure_index.items():
        logger.info('to ensure_index: key: %s', key)
        config[key].create_index(val, background=True)

    # enure unique index
    for key, val in ensure_unique_index.items():
        logger.info('to ensure_unique_index: key: %s', key)
        config[key].create_index(val, background=True, unique=True)


def clean():
    global config
    global MONGO_MAPS

    config = {}
    MONGO_MAPS = None
