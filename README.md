pyutil_mongo
==========

python util for mongodb

Usage
==========

1. Follow the following steps for initialization:

    ```
    import pyutil_mongo as util
    import logging

    logger = logging.getLogger('test')

    collection_map = {
        'a': 'b',
        'a2': 'b',
    }
    ensure_index = {
        'a': [('key1', pymongo.ASCENDING)],
        'a2': [('key2', pymongo.ASCENDING)],
    }
    ensure_unique_index = {
        'a': [('key3', pymongo.ASCENDING)],
    }

    mongo_map = util.MongoMap(collection_map, ensure_index=ensure_index, ensure_unique_index=ensure_unique_index)

    err = util.init(self.logger, [mongo_map])
    ```

2. Do all kinds of ops for the mongodb:

    ```
    # remove
    err, db_result = util.db_remove('a', {'key1': 'a'})

    # update (with $set and upsert by default)
    err, db_result = util.db_update('a', {'key1': 'a'}, {'key2': 3})

    # find-one
    err, db_result = util.db_find_one('a', {'key1': 'a'})

    # find (list)
    err, db_results = util.db_find('a', {'key1': 'a'})

    # find and get the iterator.
    err, db_it = util.db_find_it('a', {'key1': 'a'})

    # bulk-update
    err, db_result = util.db_bulk_update('a', [{'key': {'key1': 'a'}, 'val': {'key2': 4}}, {'key': {'key1': 'b'}, 'val': {'key2': '5'}}, {'key': {'key1': 'c'}, 'val': {'key2': 6}}])

    # insert
    err, db_result = util.db_insert('a', [{'key1': 'a', 'key2': 3}, {'key1': 'a', 'key2': 4}, {'key1': 'a', 'key2': 5}])

    # insert-one
    err, db_result = util.db_insert_one('a', {'key1': 'a', 'key2': 3})

    # distinct
    err, db_results = util.db_distinct('a', 'key2', {'key1': 'a'})

    # set if not exists
    err, db_result = util.db_set_if_not_exists('a', {'key1': 'a'}, {'key2': 'b'})

    # aggregate
    pipe = [{'$match': {'key1': 'a'}}, {'$group': {'_id': {'key1': '$key1'}, 'key2': {'$sum': '$key2'}}}]
    err, db_results = util.db_aggregate('a', pipe)
    err, db_results = util.db_aggregate_parse_results(db_results)

    # max
    err, db_result = util.db_max('a', 'key2', {'key1': 'a'})
    ```
