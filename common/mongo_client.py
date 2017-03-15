# -*- coding:utf-8 -*_

"""
@author: Fang Wang
@date: 2017.02.25
@desc: mongodb client
"""
import pymongo
from pymongo import MongoClient


def get_client():

    try:
        client = MongoClient(host="localhost", port=27017)
        return client
    except Exception, e:
        print "get mongodb client failed", str(e)
        raise e


def get_db(db_name="test"):

    try:
        client = get_client()
        db_con = client[db_name]
        return db_con
    except Exception, e:
        print "get db failed", str(e)
        raise e


def insert_data(args, db_name="test", collection_name="test"):

    try:
        db_con = get_db(db_name)
        collection = db_con[collection_name]
        collection.insert(args, continue_on_error=True)
        return True
    except pymongo.errors.DuplicateKeyError:
        print "duplicated keys error"
    except Exception, e:
        print "insert data failed", str(e)
        raise e


def update_item(args, db_name="test", collection_name="test"):

    try:
        db_con = get_db(db_name)
        collection = db_con[collection_name]
        collection.update(args[0], args[1], upsert=False)
    except Exception, e:
        print "update item failed", str(e)
        raise e


def query_data(args, spec={}, db_name="test", collection_name="test"):

    try:
        db_con = get_db(db_name)
        collection = db_con[collection_name]
        if not spec:
            query_result = list(collection.find(args))
        else:
            query_result = list(collection.find(args, spec))
    except Exception, e:
        print "query data failed", str(e)
        raise e

    return query_result


if __name__ == "__main__":
    print insert_data({"a": 1})
    print query_data({"a": 1})
