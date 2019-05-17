# -*- coding: utf-8 -*-

import re
import copy

from .errors import DBInvalidMongoMap, DBException, DBAlreadyExists, ErrorFlattenResults
from . import cfg


def db_find_one_ne(db_name, key, fields=None):
    """Find one data from the db, return {} if error occurred.

    Args:
        db_name (str): db-name in config
        key (dict): The selection criteria
        fields (dict, optional): Resulting fields.

    Returns:
        dict: db-result
    """
    error, result = db_find_one(db_name, key, fields)

    return result


def db_find_one(db_name, key, fields=None):
    """Find one data from the db with customized defaults

    Args:
        db_name (str): db-name in config
        key (dict): The selection criteria
        fields (dict, optional): Resulting fields.

    Returns:
        (Error, dict): db-result
    """
    if fields is None:
        fields = {'_id': False}

    error = None
    result = {}
    try:
        result = cfg.CONFIG.get(db_name).find_one(key, projection=fields)
        if not result:
            result = {}
        result = dict(result)
    except Exception as e:
        error = DBException('unable to db_find_one: db_name: %s e: %s' % (db_name, e))
        result = {}

        _db_restart_mongo(db_name, e)

    return error, result


def db_find_ne(db_name, key=None, fields=None):
    """Find data from the db, return [] if error occurred.

    Args:
        db_name (str): db-name in config
        key (dict, optional): The selection criteria
        fields (dict, optional): Resulting fields.

    Returns:
        list: db-results
    """
    error, result = db_find(db_name, key, fields)

    return result


def db_find(db_name, key=None, fields=None):
    """Find data from the db with customized defaults

    Args:
        db_name (str): db-name in config
        key (dict, optional): The selection criteria
        fields (dict, optional): Resulting fields.

    Returns:
        (Error, list): db-results
    """
    if fields is None:
        fields = {'_id': False}

    error = None
    result = []
    try:
        error, db_result_it = db_find_it(db_name, key, fields)
        result = list(db_result_it)
    except Exception as e:
        error = DBException('unable to db_find: db_name: %s e: %s' % (db_name, e))
        result = []

        _db_restart_mongo(db_name, e)

    return error, result


def db_find_it_ne(db_name, key=None, fields=None, with_id=False):
    """Find data from the db, return [] if error occurred.

    Args:
        db_name (str): db-name in config
        key (dict, optional): The selection criteria
        fields (dict, optional): Resulting fields
        with_id (bool, optional): whether to include _id forcely.

    Returns:
        iterator: db-results
    """
    error, result = db_find_it(db_name, key, fields, with_id=with_id)

    return result


def db_find_it(db_name, key=None, fields=None, with_id=False):
    """Find data from the db with customized defaults.

    Args:
        db_name (str): db-name in config
        key (dict, optional): The selection criteria
        fields (dict, optional): Resulting fields.
        with_id (bool, optional): whether to include _id forcely.

    Returns:
        (Error, iterator): db-results
    """
    if fields is None and not with_id:
        fields = {'_id': False}

    error = None
    result = []
    try:
        result = cfg.CONFIG.get(db_name).find(filter=key, projection=fields)
    except Exception as e:
        error = DBException('unable to db_find_iter: db_name: %s key: %s e: %s' % (db_name, key, e))
        result = None
        _db_restart_mongo(db_name, e)

    if not result:
        result = []

    return error, result


def db_insert_ne(db_name, val):
    """Insert data to the db

    Args:
        db_name (str): db-name in config
        val ([{}]): insert data

    Returns:
        dict: db-insert-result
    """
    error, result = db_insert(db_name, val)

    return result


def db_insert(db_name, val):
    """Insert data to the db

    Args:
        db_name (str): db-name in config
        val ([{}]): insert data

    Returns:
        dict: db-insert-result
    """
    error = None
    if not val:
        error = DBException('db_name: %s no val: val: %s' % (db_name, val))
        return error, {}

    result = []
    try:
        result = cfg.CONFIG.get(db_name).insert_many(val, ordered=False)
    except Exception as e:
        error = DBException('unable to insert: db_name: %s e: %s' % (db_name, e))
        result = []

        _db_restart_mongo(db_name, e)

    return error, result


def db_bulk_update(db_name, update_data, is_set=True, upsert=True, multi=True):
    """Bulk update with a list of update-data.

    Args:
        db_name (str): db-name in config
        update_data ([{key, val}]): list of to-update data, each includes key and val as described in :py:meth:`rx_med_analysis.util.db_update`
        is_set (bool, optional): is using set in db_update or not.
        upsert (bool, optional): is using upsert in db_update or not.
        multi (bool, optional): is using multi in db_update or not.

    Returns:
        (Error, dict): db-bulk-update-result
    """
    error = None
    update_data = [each_data for each_data in update_data if each_data.get('key', {}) and each_data.get('val', {})]

    return db_force_bulk_update(db_name, update_data, is_set=is_set, upsert=upsert, multi=multi)


def db_force_bulk_update(db_name, update_data, is_set, upsert, multi):
    """Bulk-update with a list of update-data

    Args:
        db_name (str): db-name in config
        update_data ([{key, val}]): list of to-update data, each includes key and val as described in :py:meth:`rx_med_analysis.util.db_update`
        is_set (bool): is using set in db_update or not.
        upsert (bool): is using upsert in db_update or not.
        multi (bool): is using multi in db_update or not.

    Returns:
        (Error, dict): db-bulk-update-result
    """
    error = None
    if is_set:
        for each_data in update_data:
            val = each_data.get('val', {})
            each_data['val'] = {'$set': val}

    result = None
    try:
        bulk = cfg.CONFIG.get(db_name).initialize_unordered_bulk_op()
        for each_data in update_data:
            key = each_data.get('key', {})
            val = each_data.get('val', {})
            if upsert and multi:
                bulk.find(key).upsert().update(val)
            elif upsert:
                # upsert only
                bulk.find(key).upsert().update_one(val)
            elif multi:
                # multi only
                bulk.find(key).update(val)
            else:
                # no upsert and no multi
                bulk.find(key).update_one(val)
        result = bulk.execute()
    except Exception as e:
        error = DBException('unable to db_force_bulk_update: db_name: %s e: %s' % (db_name, e))
        result = None

        _db_restart_mongo(db_name, e)

    return error, getattr(result, 'raw_result', {})


def db_update(db_name, key, val, is_set=True, upsert=True, multi=True):
    """update data

    Args:
        db_name (str): db-name in config
        key (dict): the selection criteria
        val (dict): to-update data
        is_set (bool, optional): is using set in db_update or not.
        upsert (bool, optional): is using upsert in db_update or not.
        multi (bool, optional): is using multi in db_update or not.

    Returns:
        (Error, dict): db-update-result
    """
    error = None

    if not key or not val:
        error = DBException('unable to db_update: no key or val: db_name: %s' % (db_name))
        return error, {}

    return db_force_update(db_name, key, val, is_set=is_set, upsert=upsert, multi=multi)


def db_force_update(db_name, key, val, is_set=True, upsert=True, multi=True):
    """udpate data

    Args:
        db_name (str): db-name in config
        key (dict): the selection criteria
        val (dict): to-update data
        is_set (bool, optional): is using set in db_update or not.
        upsert (bool, optional): is using upsert in db_update or not.
        multi (bool, optional): is using multi in db_update or not.

    Returns:
        (Error, dict): db-update-result
    """
    error = None

    if is_set:
        val = {"$set": val}

    result = None
    try:
        if not multi:
            result = cfg.CONFIG.get(db_name).update_one(key, val, upsert=upsert)
        else:
            result = cfg.CONFIG.get(db_name).update_many(key, val, upsert=upsert)
    except Exception as e:
        error = DBException('unable to db_force_update: db_name: %s e: %s' % (db_name, e))
        result = None

        _db_restart_mongo(db_name, e)

    return error, getattr(result, 'raw_result', {})


def db_insert_one(db_name, doc):
    """Insert one doc.

    Args:
        db_name (str): db-name in config
        doc (dict): save data

    Returns:
        (Error, dict): db-save-result
    """
    error = None

    if not doc:
        error = DBException('db_insert_one: no doc: db_name: %s' % (db_name))
        return error, {}

    return db_insert(db_name, [doc])


def db_remove(db_name, key):
    """Remove data

    Args:
        db_name (str): db-name in config
        key (dict): the selection criteria

    Returns:
        (Error, dict): db-remove-result
    """
    error = None

    if not key:
        error = DBException('unable to db_remove: no key: db_name: %s' % (db_name))
        return error, {}

    return db_force_remove(db_name, key=key)


def db_force_remove(db_name, key=None):
    """Remove data

    Args:
        db_name (str): db-name in config
        key (dict, optional): the selection criteria

    Returns:
        (Error, dict): db-remove-result
    """
    if not key:
        key = {}

    error = None

    result = None
    try:
        result = cfg.CONFIG.get(db_name).delete_many(key)
    except Exception as e:
        error = DBException('unable to db_force_remove: db_name: %s key: %s e: %s' % (db_name, key, e))
        result = None

        _db_restart_mongo(db_name, e)

    return error, getattr(result, 'raw_result', {})


def db_distinct(db_name, distinct_key, query_key, fields=None, with_id=False):
    """Distinct data

    Args:
        db_name (str): db-name in config
        distinct_key (str): the distinct key
        query_key (dict): the selection criteria
        fields (dict, optional): the resulting fields
        with_id (bool, optional): whether include _id forcely.

    Returns:
        (Error, list): db-distinct-results
    """
    if fields is None and not with_id:
        fields = {'_id': False}

    error = None

    results = []
    try:
        db_result = cfg.CONFIG.get(db_name).find(query_key, projection=fields)
        results = db_result.distinct(distinct_key)
    except Exception as e:
        error = DBException('unable to db_distinct: db_name: %s query_key: %s distinct_key: %s e: %s' % (db_name, query_key, distinct_key, e))
        results = []

        _db_restart_mongo(db_name, e)

    return error, results


def db_set_if_not_exists(db_name, key, val, fields=None, with_id=False):
    """Summary

    Args:
        db_name (str): db-name in config
        key (dict): the selection critertia
        val (dict): to-update-data
        fields (dict, optional): resulting fields
        with_id (bool, optional): whether include _id forcely.

    Returns:
        (Error, dict): db-set-if-not-exists-result
    """
    if fields is None and not with_id:
        fields = {'_id': False}

    error = None
    result = {}
    try:
        result = cfg.CONFIG.get(db_name).find_one_and_update(key, {"$setOnInsert": val}, projection=fields, upsert=True)
    except Exception as e:
        error = DBException('unable to set on insert: db_name: %s key: %s e: %s' % (db_name, key, e))
        result = {}

        _db_restart_mongo(db_name, e)

    if error:
        return error, {}

    if result:
        return DBAlreadyExists('already exists: db_name: %s key: %s' % (db_name, key)), result

    return None, {}


def db_find_and_modify(db_name, key, val, fields=None, with_id=False, is_set=True, upsert=True, multi=True):
    """find and modify

    Args:
        db_name (str): db-name in config
        key (dict): the selection criteria
        val (dict): to-update data
        fields (dict, optional): resulting fields
        with_id (bool, optional): whether to forcely including _id in resulting fields.
        is_set (bool, optional): is using set in db_update or not.
        upsert (bool, optional): is using upsert in db_update or not.
        multi (bool, optional): is using multi in db_update or not.

    Returns:
        (Error, dict): db-find-and-modify-result
    """
    if fields is None and not with_id:
        fields = {'_id': False}

    error = None

    if is_set:
        val = {'$set': val}

    result = {}
    try:
        result = cfg.CONFIG.get(db_name).find_one_and_update(key, val, projection=fields, upsert=upsert, multi=multi)
        if not result:
            result = {}
    except Exception as e:
        error = DBException('unable to db_find_and_modify: db_name: %s key: %s val: %s e: %s' % (db_name, key, val, e))
        result = {}

        _db_restart_mongo(db_name, e)

    return error, dict(result)


def db_aggregate_iter(db_name, pipe):
    """db-aggregate

    Args:
        db_name (str): db-name in config
        pipe ([{}]): pipe in db-aggregate

    Returns:
        (Error, iterator): db-aggregate-results
    """
    error = None

    db_result = []
    try:
        db_result = cfg.CONFIG.get(db_name).aggregate(pipeline=pipe, cursor={}, allowDiskUse=True)
    except Exception as e:
        error = DBException('unable to db_aggregate_iter: possibly 2.4 issue db_name: %s pipe: %s e: %s' % (db_name, pipe, e))
        db_result = []

    if not error:
        return error, db_result

    try:
        db_result = cfg.CONFIG.get(db_name).aggregate(pipeline=pipe)
        db_result = db_result.get('result', [])
    except Exception as e:
        error = DBException('unable to db_aggregate_iter: db_name: %s pipe: %s e: %s' % (db_name, pipe, e))
        db_result = []

        _db_restart_mongo(db_name, e)

    return error, db_result


def db_aggregate(db_name, pipe):
    """db-aggregate

    Args:
        db_name (str): db-name in config
        pipe ([{}]): pipe in db-aggregate

    Returns:
        (Error, list): db-aggregate-results
    """
    error = None

    error, db_result = db_aggregate_iter(db_name, pipe)
    if error:
        return error, []

    result = []
    try:
        result = list(db_result)
    except Exception as e:
        result = []
        error = DBException('unable to db_aggregate: db_anme: %s pipe: %s e: %s' % (db_name, pipe, e))

        _db_restart_mongo(db_name, e)

    return error, result


def db_aggregate_parse_results(db_results):
    '''
    db_aggregate_parse_result

    Args:
        db_results (TYPE): Description

    Returns:
        TYPE: Description
    '''
    results_with_error = [db_aggregate_parse_result(db_result) for db_result in db_results]

    return _flatten_results_with_error(results_with_error)


def db_aggregate_parse_result(db_result):
    """Parse each db-result from db-aggregate by integrating db_result['_id'] into db_result

    Args:
        db_result (dict): db-result from db_aggregate

    Returns:
        dict: integrated result
    """

    result = copy.deepcopy(db_result)
    if '_id' in result:
        data_id = result['_id']
        result.update(data_id)
        del result['_id']

    return None, result


def db_max(db_name, key, query, group_columns=None):
    """Find largest record in the db, return as dict

    Args:
        db_name (str): db-name in config
        key (str): the key for largest
        query (dict): the selection criteria
        group_columns (list, optional): group columns in db-aggregate

    Returns:
        (Error, dict): largest record
    """
    error, db_results = _db_max_list(db_name, key, query, group_columns)
    if error:
        return error, db_results

    if not db_results:
        return DBException('[empty]'), {}

    return None, db_results[0]['max']


def _db_max_list(db_name, key, query, group_columns=None):
    """Find largest record in the db, return as list

    Args:
        db_name (str): db-name in config
        key (str): key for largest
        query (dict): the selection criteria
        group_columns (list, optional): group-by column in _id

    Returns:
        (Error, list): largest record
    """
    if not group_columns:
        group_columns = query.keys()

    group = {}
    group['max'] = {'$max': '$' + key}
    group['_id'] = {column: '$' + column for column in group_columns}
    pipe = [
        {'$match': query},
        {'$group': group},
    ]

    error, results = db_aggregate(db_name, pipe)

    if error:
        return error, []

    return None, results


def _db_restart_mongo(db_name, e):
    """Restart mongo with the corresponding db-name


    Restart mongo with the corresponding db-name except the following error:
    * E11000: duplicate-error in unique-index-key

    Args:
        db_name (str): db-name in config
        e (Exception): exception

    Returns:
        None: None
    """
    e_str = str(e)

    # ignore dup error
    if re.search('^E11000', e_str):
        cfg.LOGGER.debug('E11000')
        return None

    cfg.LOGGER.debug('to restart mongo')

    cfg.restart_mongo(db_name)

    return None


def drop(db_name):
    error = None
    try:
        cfg.CONFIG.get(db_name).drop()
    except Exception as e:
        error = DBException('unable to db_drop: db_name: %s e: %s' % (db_name, e))

        _db_restart_mongo(db_name, e)

    return error


def _flatten_results_with_error(results_with_error):
    """flatten results with error

    Args:
        results_with_error ([(Error, result)]): results with error

    Returns:
        (Error, [result]): error, results
    """
    error_msg_list = []
    results = []
    for idx, (each_error, each_result) in enumerate(results_with_error):
        if each_error:
            error_msg_list.append('(%s/%s) e: %s' % (idx, len(results_with_error), each_error))
        results.append(each_result)

    error = None if not error_msg_list else ErrorFlattenResults(','.join(error_msg_list))
    if error:
        return error, results

    return None, results