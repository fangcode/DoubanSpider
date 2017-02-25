# -*- coding:utf-8 -*-

import requests
from common.mongo_client import insert_data
from lxml import html


def crawl_content(tag_url):

    try:
        r = requests.get(tag_url)
        tag_content = r.text
    except Exception, e:
        print "get tag failed", str(e)
        raise e

    return tag_content


def parse_tags(content):
    tags_result = list()

    try:
        root = html.fromstring(content)
        content_ele = root.find_class("article")[0].find("./div[2]")
        content_ele_list = content_ele.getchildren()

        for each_content_ele in content_ele_list:
            cate_name = each_content_ele.xpath("a/@name")[0]

            content_col_list = each_content_ele.xpath("table/tbody/tr")

            for each_content_col in content_col_list:
                content_row_list = each_content_col.xpath("./td")

                for each_tag_ele in content_row_list:
                    tag_name = each_tag_ele.xpath("./a")[0].text_content()
                    tag_url_tmp = each_tag_ele.xpath("./a/@href")[0]
                    tag_url = "https://book.douban.com" + tag_url_tmp

                    tag_code_text = each_tag_ele.xpath("./b")[0].text_content()
                    tag_code = tag_code_text.strip()[1:-1]
                    each_tag_dict = {
                        "tag_name": tag_name,
                        "tag_url": tag_url,
                        "tag_code": tag_code,
                        "cate": cate_name,
                        "status": 0
                    }
                    tags_result.append(each_tag_dict)
    except Exception, e:
        print "parse tag content failed", str(e)

    return tags_result


def crawl_tag():

    tag_url = "https://book.douban.com/tag/?icn=index-nav"

    try:
        tag_content = crawl_content(tag_url)
        tags_list = parse_tags(tag_content)

        db_name = "douban"
        collection_name = "tag"

        for each_tag_data in tags_list:
            try:
                insert_flag = insert_data(each_tag_data, db_name, collection_name)
                if not insert_flag:
                    continue
            except Exception, e:
                print "insert data failed", str(e)
                continue
    except Exception, e:
        print "crawl tags failed", str(e)
        raise e


if __name__ == "__main__":
    crawl_tag()
