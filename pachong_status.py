# -*- coding: utf-8 -*-

import pymongo
import time
import re
from multiprocessing import Pool
import random

db_house = 'house'

C_DAY = "%02d_%02d_%02d" % (time.localtime().tm_year, time.localtime().tm_mon, time.localtime().tm_mday)

PAGE_GONE_STATE = 'gone state'
FORBIDDEN_STATE = 'forbidden state'
LAST_PAGE_STATE = 'last page state'
NORMAL_STATE = 'normal state'

# 检查网页是否被冻结，或者是否下架了,或者最后一页
def get_page_state(soup):
    if '访问验证-安居客' in soup.title.text:
        print('访问受限制了，需要手动验证')
        time.sleep(10)
        return FORBIDDEN_STATE
    elif '您要浏览的网页可能被删除' in soup.title.text:
        print('房屋被删除了')
        return PAGE_GONE_STATE

    elif len(soup.select('#content > div.sale-left > div.filter-no')) != 0:
        print('没有合适的房源了！')
        return LAST_PAGE_STATE
    elif len(soup.select('body > div.expire-content > div.expire-main > div.expire-similar-mod > div.hd > span')) != 0:
        print('房屋卖出或者过期了')
        return PAGE_GONE_STATE
    return NORMAL_STATE


# 获取数据爬取的状态，和当前日期有关
def get_total_collect_satus():
    client = pymongo.MongoClient('localhost', 27017, connect=False)

    house = client[db_house]
    status = house['采集状态页']
    url_list = house['网址列表页']

    # 没有记录，就插入一条
    if status.count() == 0:
        status.insert_one({'采集状态行': True, '列表是否完整': False, '采集日期': C_DAY})

    # 列表不完整，就删除掉列表
    if status.find_one()['列表是否完整'] == False:
        for url in url_list.find():
            url_list.update_one({'网址': url['网址']}, {'$set': {'采集完毕': False}})
        pass

    # 和上次更新日期不同，就删除列表，同时刷新本次日期
    if status.find_one()['采集日期'] != C_DAY:
        # 把url_list中的完成标识设置为false
        # house.drop_collection('网址列表页')
        for url in url_list.find():
            url_list.update_one({'网址': url['网址']}, {'$set': {'采集完毕': False}})
        status.remove()
        status.insert_one({'采集状态行': True, '列表是否完整': False, '采集日期': C_DAY})

    return status


# 设置爬虫的状态，主要是刷新采集的日期，以及网址列表是否完整
def set_total_collect_satus(newRec):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    status = house['采集状态页']
    status.remove({})
    status.insert_one(newRec)
    return

def set_house_collect_satus(url):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list.update_one({'网址': url}, {'$set': {'采集完毕': True}}, upsert=True)
    return


def insert_url_to_db(record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    if url_list.find_one({'网址': record}) == None:
        url_list.insert_one(record)

def check_if_page_collected(url):
    # 先看该网址是否已经采集过了
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']

    lp_info_record = url_list.find_one({'网址': url})
    if lp_info_record['采集完毕'] == True:
        return True
    else:
        return False


