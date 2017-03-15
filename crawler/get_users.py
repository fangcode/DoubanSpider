# -*- coding:utf-8 -*-

import sys
sys.path.append("../")
import time
import random
import requests
from lxml import html
from common.mongo_client import query_data
from common.mongo_client import insert_data
from common.mongo_client import update_item


TYPE_LIST = ["doings", "collections", "wishes"]


def get_users():

    condition = {"user_status": 0}
    spec = {"book_url": 1, "_id": 0}
    db_name = "douban"
    collection_name = "booklist"

    try:
        result = query_data(condition, spec, db_name, collection_name)
    except Exception, e:
        print "query book list failed", str(e)
        raise

    return result


def crawl_each_book(book_entry):

    book_url = book_entry.get("book_url")
    s = requests.Session()
    s.get(book_url)
    finished_status = False
    user_info_list = list()
    failed_times = 0

    for each_type in TYPE_LIST:
        finished_status = False
        next_url = book_url + each_type

        while True:
            for crawl_time in range(3):
                time.sleep(random.randint(6, 12))
                print next_url
                req = s.get(next_url)
                content = req.text
                user_info_list_on_page, next_url, finished_status = parse_users(content, next_url)
                user_info_list += user_info_list_on_page
                if user_info_list_on_page or finished_status or not next_url.strip():
                    break

                failed_times += 1
                if failed_times >= 15:
                    return user_info_list, False

            if finished_status:
                break

    return user_info_list, finished_status


def parse_users(content, current_url):
    user_info_list = list()
    next_url = ""
    finished_status = False

    try:
        root = html.fromstring(content)
        user_ele_list = root.xpath("//div[contains(@class, 'sub_ins')][1]/table")
    except Exception, e:
        print "parse page failed", str(e)
        return user_info_list, next_url, finished_status

    for each_user_ele in user_ele_list:
        try:
            user_url = each_user_ele.xpath(".//div[contains(@class, 'pl2')][1]/a/@href")[0]
            user_info_list.append({"user_url": user_url,
                                   "status": 0,
                                   "login": 0})
            # print html.tostring(each_user_ele)
        except Exception, e:
            print "parsing user info failed", str(e)

    try:
        next_url = root.xpath("//span[contains(@class, 'next')]/a/@href")[0]
    except Exception, e:
        if user_info_list or "180" in current_url:
            finished_status = True
        print "parse next page url failed", str(e)

    return user_info_list, next_url, finished_status


def crawl_users():
    book_list = list()

    try:
        book_list = get_users()
    except Exception, e:
        print "query book list failed", str(e)

    for each_book_entry in book_list:
        crawled_status = False
        try:
            user_info_list, crawled_status = crawl_each_book(each_book_entry)
            insert_data(user_info_list, db_name="douban", collection_name="userlist")

            if crawled_status:
                update_item_info = {"book_url": each_book_entry.get("book_url")}
                op_info = {"$set": {"user_status": 1}, }
                update_item((update_item_info, op_info),
                            db_name="douban", collection_name="booklist")
        except Exception, e:
            print "crawling ", each_book_entry, str(e)

        if not crawled_status:
            break


if __name__ == "__main__":
    crawl_users()
