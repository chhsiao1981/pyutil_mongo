# -*- coding: utf-8 -*-


class DBInvalidMongoMap(Exception):
    def __init__(self):
        super().__init__('invalid mongo map')

class DBException(Exception):
    def __init__(self):
        super().__init__('invalid db')

class DBAlreadyExists(Exception):
    pass

class ErrorFlattenResults(Exception):
    def __init__(self, the_str):
        super().__init__(the_str)
