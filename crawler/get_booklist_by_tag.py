# -*- coding:utf-8 -*-

import requests
from common.mongo_client import query_data
from common.mongo_client import insert_data
from lxml import html
import traceback


FINISHED_TAG = "没有找到符合条件的图书"
FINISHED_COUNT = 20


def get_tags():

    condition = {"status": 0}
    db_name = "douban"
    collection_name = "tag"

    try:
        result = query_data(condition, db_name, collection_name)
    except Exception, e:
        print "query tag failed", str(e)
        raise

    return result


def parse_booklist(tag_content):

    booklist = list()
    side_tag_list = list()
    next_url = ""
    finished_tag = False

    try:
        if FINISHED_TAG in tag_content:
            return finished_tag, next_url, booklist, side_tag_list

        root = html.fromstring(content)

        # parse book information list
        tag_list = root.xpath("//ul[contains(@class, 'subject-list')][1]/li")

        for each_tag in tag_list:
            book_url = each_tag.xpath("./div[1]/a/@href")[0]
            book_name = each_tag.xpath("./div[contains(@class, 'info')]/h2/a/@title")[0]
            rate_num = each_tag.xpath(".//span[contains(@class, 'rating_nums')]/text()")[0]
            booklist.append({
                "book_url": book_url,
                "book_name": book_name,
                "rate_num": rate_num,
                "status": 0
            })

        # parse side tag
        try:
            side_tag_ele_list = root.xpath("//div[contains(@class, 'tags-list')]/a")
            for each_sid_tag_ele in side_tag_ele_list:
                side_tag_name = each_sid_tag_ele.xpath("./text()")[0]
                side_tag_url = each_sid_tag_ele.xpath("./@href")[0]
                side_tag_url = "https://book.douban.com" + side_tag_url
                side_tag_list.append({
                    "tag_name": side_tag_name,
                    "tag_url": side_tag_url,
                    "tag_code": "0",
                    "cate": "NULL",
                    "status": 0,
                    "source": "side"
                })
        except Exception, e:
            print "parse side tag list failed", str(e)

        # parse next url
        try:
            next_url = root.xpath("//div[contains(@class, 'paginator')]/span[contains(@class, 'next')]/link/@href")[0]
            if "1000" in next_url:
                finished_tag = True
            next_url = "https://book.douban.com" + next_url
        except Exception, e:
            print "parse next page url failed", str(e)

    except Exception, e:
        traceback.print_exc(e)

    return finished_tag, next_url, booklist, side_tag_list


def crawl_booklist_by_tag(tag_info):
    booklist = list()

    tag_url = tag_info.get("tag_url")

    # create session
    s = requests.Session()
    s.get("https://book.douban.com/tag/?icn=index-nav")

    next_url = tag_url

    # crawl tag content
    while True:
        print next_url

        for page_index in range(3):
            try:
                req = s.get(next_url)
                tag_content = req.text
                
                return True
            except Exception, e:
                print "crawling ", tag_url.encode("utf-8"), "failed"
                continue

    return True


def crawl_booklist():

    try:
        tag_info_list = get_tags()
    except Exception, e:
        print "get tags failed", str(e)
        raise e

    for each_tag_info in tag_info_list[:1]:
        for times in range(3):
            try:
                crawl_flag = crawl_booklist_by_tag(each_tag_info)
                if crawl_flag:
                    break
            except Exception, e:
                print "crawl tag failed", str(each_tag_info), str(e)
                continue


if __name__ == "__main__":
    with open("book.html") as f:
        content = f.read()
        print parse_booklist(content)
