# -*- coding: utf-8 -*-
"""Summary

Attributes:
    config (dict): Description
    logger (logging.Logger): Description
"""
import logging
from pymongo import MongoClient

logger = None
config = {}


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
    db_name = ""
    hostname = ""
    mongo_db_name = ""
    ssl = False
    cert = None
    ca = None
    is_default= False

    def __init__(self, collection_map: dict, ensure_index=None, ensure_unique_index=None, db_name="mongo", hostname="localhost:27017", mongo_db_name="test", ssl=False, cert=None, ca=None, is_default=False):
        """Summary

        Args:
            collection_map (dict): Description
            ensure_index (None, optional): Description
            ensure_unique_index (None, optional): Description
            name (str, optional): Description
            hostname (str, optional): Description
            db (str, optional): Description
        """
        self.db_name = db_name
        self.hostname = hostname
        self.mongo_db_name = mongo_db_name
        self.collection_map = collection_map
        self.ensure_index = ensure_index
        self.ensure_unique_index = ensure_unique_index
        self.ssl = ssl
        self.cert = cert
        self.ca = ca
        self.is_default = is_default


def init(the_logger: logging.Logger, mongo_maps: list):
    """Summary

    Args:
        logger (logging.Logger): Description
        mongo_maps (list[MongoMap]): Description
    """
    global logger

    logger = the_logger

    return restart_mongo(mongo_maps=mongo_maps)


def restart_mongo(collection_name="", db_name="", mongo_maps=None):
    """Summary

    Args:
        collection_name (str, optional): Description

    Returns:
        TYPE: Description
    """
    '''
    initialize mongo
    '''
    global config

    if mongo_maps is None:
        mongo_maps = [each['mongo_map'] for each in config.values()]

    if len(mongo_maps) == 0:
        return

    errs = []
    for idx, mongo_map in enumerate(mongo_maps):
        each_err = _init_mongo_map_core(mongo_map, collection_name=collection_name, db_name=db_name)
        if each_err:
            errs.append(each_err)
            logger.error('(%s/%s): e: %s', idx, len(mongo_maps), each_err)

    if not errs:
        return None

    err_str = ','.join(['%s' % (each) for each in errs])

    return Exception(err_str)


def _init_mongo_map_core(mongo_map: MongoMap, collection_name="", db_name=""):
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

    mongo_map_db_name, hostname, mongo_db_name, collection_map, ensure_index, ensure_unique_index = mongo_map.db_name, mongo_map.hostname, mongo_map.mongo_db_name, mongo_map.collection_map, mongo_map.ensure_index, mongo_map.ensure_unique_index

    if db_name != '' and mongo_map_db_name != db_name:
        return

    if collection_name != '' and collection_name not in collection_map:
        return

    if collection_name == '' and mongo_map_db_name in config:
        return Exception('db already in config: db_name: %s config: %s', mongo_map_db_name, config[mongo_map_db_name])

    if ensure_index is None:
        ensure_index = {}

    if ensure_unique_index is None:
        ensure_unique_index = {}

    # mongo_server_url
    mongo_server_url = "mongodb://" + hostname + "/" + mongo_db_name

    # mongo-server-client
    mongo_kwargs = {}
    if mongo_map.ssl:
        mongo_kwargs.update({
            'ssl': True,
            'authSource': '$external',
            'authMechanism': 'MONGODB-X509',
            'ssl_certfile': mongo_map.cert,
            'ssl_ca_certs': mongo_map.ca,
        })

    mongo_server_client = MongoClient(
        mongo_server_url,
        **mongo_kwargs,
    )[mongo_db_name]

    # config-by-db-name
    config_by_db_name = {'mongo_map': mongo_map, 'db': {}, 'url': mongo_server_url}

    # collection
    for (key, val) in collection_map.items():
        logger.info('mongo: %s => %s', key, val)
        config_by_db_name['db'][key] = mongo_server_client[val]

    # enure index
    for key, val in ensure_index.items():
        logger.info('to ensure_index: key: %s', key)
        config_by_db_name['db'][key].create_index(val, background=True)

    # enure unique index
    for key, val in ensure_unique_index.items():
        logger.info('to ensure_unique_index: key: %s', key)
        config_by_db_name['db'][key].create_index(val, background=True, unique=True)

    config[mongo_map_db_name] = config_by_db_name
    if mongo_map.is_default:
        config['_default_db'] = mongo_map_db_name


def clean():
    global config

    config = {}
