# -*- coding: utf-8 -*-

import re
import copy

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
    err, result = db_find_one(db_name, key, fields)

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

    err = None
    result = {}
    try:
        result = cfg.config.get(db_name).find_one(key, projection=fields)
        if not result:
            result = {}
        result = dict(result)
    except Exception as e:
        err = e
        result = {}

        _db_restart_mongo(db_name, e)

    return err, result


def db_find_ne(db_name, key=None, fields=None):
    """Find data from the db, return [] if error occurred.

    Args:
        db_name (str): db-name in config
        key (dict, optional): The selection criteria
        fields (dict, optional): Resulting fields.

    Returns:
        list: db-results
    """
    err, result = db_find(db_name, key, fields)

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

    err = None
    result = []
    try:
        err, db_result_it = db_find_it(db_name, key, fields)
        result = list(db_result_it)
    except Exception as e:
        err = e
        result = []

        _db_restart_mongo(db_name, e)

    return err, result


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
    err, result = db_find_it(db_name, key, fields, with_id=with_id)

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

    err = None
    result = []
    try:
        result = cfg.config.get(db_name).find(filter=key, projection=fields)
    except Exception as e:
        err = e
        result = None
        _db_restart_mongo(db_name, e)

    if not result:
        result = []

    return err, result


def db_insert_ne(db_name, val):
    """Insert data to the db

    Args:
        db_name (str): db-name in config
        val ([{}]): insert data

    Returns:
        dict: db-insert-result
    """
    err, result = db_insert(db_name, val)

    return result


def db_insert(db_name, val):
    """Insert data to the db

    Args:
        db_name (str): db-name in config
        val ([{}]): insert data

    Returns:
        dict: db-insert-result
    """
    err = None
    if not val:
        err = Exception('db_name: %s no val: val: %s' % (db_name, val))
        return err, {}

    result = []
    try:
        result = cfg.config.get(db_name).insert_many(val, ordered=False)
    except Exception as e:
        err = e
        result = []

        _db_restart_mongo(db_name, e)

    return err, result


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
    err = None
    if is_set:
        for each_data in update_data:
            val = each_data.get('val', {})
            each_data['val'] = {'$set': val}

    result = None
    try:
        bulk = cfg.config.get(db_name).initialize_unordered_bulk_op()
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
        err = e
        result = None

        _db_restart_mongo(db_name, e)

    return err, getattr(result, 'raw_result', {})


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
    err = None

    if not key or not val:
        err = Exception('unable to db_update: no key or val: db_name: %s' % (db_name))
        return err, {}

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
    err = None

    if is_set:
        val = {"$set": val}

    result = None
    try:
        if not multi:
            result = cfg.config.get(db_name).update_one(key, val, upsert=upsert)
        else:
            result = cfg.config.get(db_name).update_many(key, val, upsert=upsert)
    except Exception as e:
        err = e
        result = None

        _db_restart_mongo(db_name, e)

    return err, getattr(result, 'raw_result', {})


def db_insert_one(db_name, doc):
    """Insert one doc.

    Args:
        db_name (str): db-name in config
        doc (dict): save data

    Returns:
        (Error, dict): db-save-result
    """
    err = None

    if not doc:
        err = Exception('db_insert_one: no doc: db_name: %s' % (db_name))
        return err, {}

    return db_insert(db_name, [doc])


def db_remove(db_name, key):
    """Remove data

    Args:
        db_name (str): db-name in config
        key (dict): the selection criteria

    Returns:
        (Error, dict): db-remove-result
    """
    err = None

    if not key:
        err = Exception('unable to db_remove: no key: db_name: %s' % (db_name))
        return err, {}

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

    err = None

    result = None
    try:
        result = cfg.config.get(db_name).delete_many(key)
    except Exception as e:
        err = e
        result = None

        _db_restart_mongo(db_name, e)

    return err, getattr(result, 'raw_result', {})


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

    err = None

    results = []
    try:
        db_result = cfg.config.get(db_name).find(query_key, projection=fields)
        results = db_result.distinct(distinct_key)
    except Exception as e:
        err = e
        results = []

        _db_restart_mongo(db_name, e)

    return err, results


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

    err = None
    result = {}
    try:
        result = cfg.config.get(db_name).find_one_and_update(key, {"$setOnInsert": val}, projection=fields, upsert=True)
    except Exception as e:
        err = e
        result = {}

        _db_restart_mongo(db_name, e)

    if err:
        return err, {}

    if result:
        return Exception('already exists: db_name: %s key: %s' % (db_name, key)), result

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

    err = None

    if is_set:
        val = {'$set': val}

    result = {}
    try:
        result = cfg.config.get(db_name).find_one_and_update(key, val, projection=fields, upsert=upsert, multi=multi)
        if not result:
            result = {}
    except Exception as e:
        err = e
        result = {}

        _db_restart_mongo(db_name, e)

    return err, dict(result)


def db_aggregate_iter(db_name, pipe):
    """db-aggregate

    Args:
        db_name (str): db-name in config
        pipe ([{}]): pipe in db-aggregate

    Returns:
        (Error, iterator): db-aggregate-results
    """
    err = None

    db_result = []
    try:
        db_result = cfg.config.get(db_name).aggregate(pipeline=pipe, cursor={}, allowDiskUse=True)
    except Exception as e:
        err = e
        db_result = []

    return err, db_result


def db_aggregate(db_name, pipe):
    """db-aggregate

    Args:
        db_name (str): db-name in config
        pipe ([{}]): pipe in db-aggregate

    Returns:
        (Error, list): db-aggregate-results
    """
    err = None

    err, db_result = db_aggregate_iter(db_name, pipe)
    if err:
        return err, []

    result = []
    try:
        result = list(db_result)
    except Exception as e:
        result = []
        err = e

        _db_restart_mongo(db_name, e)

    return err, result


def db_aggregate_parse_results(db_results):
    '''
    db_aggregate_parse_result

    Args:
        db_results (TYPE): Description

    Returns:
        TYPE: Description
    '''
    results_with_err = [db_aggregate_parse_result(db_result) for db_result in db_results]

    return _flatten_results_with_err(results_with_err)


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
    err, db_results = _db_max_list(db_name, key, query, group_columns)
    if err:
        return err, db_results

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

    err, results = db_aggregate(db_name, pipe)

    if err:
        return err, []

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
        cfg.logger.debug('E11000: e: %s', e)
        return None

    cfg.logger.debug('to restart mongo: e: %s', e)

    cfg.restart_mongo(db_name)

    return None


def drop(db_name):
    err = None
    try:
        cfg.config.get(db_name).drop()
    except Exception as e:
        err = e

        _db_restart_mongo(db_name, e)

    return err


def _flatten_results_with_err(results_with_err):
    """flatten results with error

    Args:
        results_with_err ([(Error, result)]): results with error

    Returns:
        (Error, [result]): error, results
    """
    err_msg_list = []
    results = []
    for idx, (each_err, each_result) in enumerate(results_with_err):
        if each_err:
            err_msg_list.append('(%s/%s) e: %s' % (idx, len(results_with_err), each_err))
        results.append(each_result)

    err = None if not err_msg_list else Exception(','.join(err_msg_list))
    if err:
        return err, results

    return None, results
