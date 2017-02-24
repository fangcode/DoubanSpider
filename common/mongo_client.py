# -*- coding:utf-8 -*_

"""
@author: Fang Wang
@date: 2017.02.25
@desc: mongodb client
"""
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


def insert_data(args, db_name="test",collection_name="test"):

    try:
        db_con = get_db(db_name)
        collection = db_con[collection_name]
        collection.insert(args)
        return True
    except Exception, e:
        print "insert data failed", str(e)
        raise e


if __name__ == "__main__":
    print insert_data({"a": 1})