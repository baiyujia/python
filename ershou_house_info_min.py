# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pymongo
import time
import re
from pandas import Series, DataFrame
import pandas as pd
from multiprocessing import Pool
import random
from pachong_status import *
from matplotlib.font_manager import FontManager, FontProperties
import matplotlib
import sys
import os
import threading

headers = {
    'cookies': 'ctid=31; aQQ_ajkguid=D0174100-3E84-3050-1537-SX0626220205; sessid=F458D7EC-B0CE-6BA4-C930-SX0626220205; isp=true; twe=2; 58tj_uuid=2f50f7d6-4abf-4c60-8eae-cbaf641f9580; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1530021729; als=0; ajk_member_captcha=38a0011b908a1c16fca40584e66cf3ab; lp_lt_ut=a34ae0a5041a1ac2ab5058b3ff39ac1d; lps=http%3A%2F%2Fxa.anjuke.com%2Fsale%2Fp1%2F%7C; init_refer=; new_uv=7; _ga=GA1.2.466653724.1530276981; _gid=GA1.2.612205714.1530276981; browse_comm_ids=398617; new_session=0; propertys=kxqo1b-pb35gy_; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1530277715; _gat=1; __xsptplusUT_8=1; __xsptplus8=8.5.1530276981.1530277735.10%234%7C%7C%7C%7C%7C%23%23LW_EYiniJbsOE0diRvFfQzQG3HUoWAyX%23',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

proxy_list = [
]

db_house = 'house'
PIC_PATH = '/Users/zhlsuo/Desktop/房价走势图/'
#url_address_format = 'https://xa.anjuke.com/sale/p{}/'
url_address_format = 'https://xa.anjuke.com/sale/beilinqu/p{}/'

houseinfo_selector = '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-wrap > div > div dd'


def insert_houseinfo_to_db(info_record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']
    old_record = lp_info.find_one({'网址': info_record['网址']})
    if old_record != None:
        old_record['总价_' + C_DAY] = info_record['总价_' + C_DAY]
        lp_info.update_one({'_id': old_record['_id']}, {'$set': old_record})
    else:
        lp_info.update_one({'网址': info_record['网址']}, {'$set': info_record}, upsert=True)
        print('插入新数据', info_record['标题'])
    # db_record = lp_info.find_one({'网址': info_record['网址']})
    # if db_record == None:
    #     lp_info.insert_one(info_record)
    #     # print('获取房屋:{}'.format(info_record['标题']))
    # else:
    #     lp_info.update_one({'网址': info_record['网址']}, {'$set': info_record},upsert=True)
    # print('更新房屋:{}'.format(info_record['标题']))


def getChineseFont():
    return FontProperties(fname='/Library/Fonts/Songti.ttc')

def get_collect_status(url):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    record = url_list.find_one({'网址': url})
    if record != None and record['采集完毕'] == False:
        return True
    return False
# 根据页面解析出网址列表
def parse_house_urls(page):
    delaytime = random.randint(1, 3)
    time.sleep(delaytime)
    # proxy = random.choice(proxy_list)
    # proxies = {'http': proxy}
    wb_data = requests.get(page, headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')

    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return pagestate

    infos = soup.select('#houselist-mod-new > li')
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']

    url_list_to_be_get = {url['网址']  for url  in url_list.find() if url['采集完毕'] == False and url.get('已过期',False) == False}

    #''楼盘名称', 1:'地理位置',2:'交房时间',3:'住宅类型',4:'户型',5:'面积',6:'方向',7:'层数',8:'单价',9:'首付',10:'月供',11:'装修程度'}
    for info in infos:
        h_info = {}
        h_info['网址'] = info.select('a.houseListTitle')[0].get('href').split('?')[0]
        if h_info['网址'] not in url_list_to_be_get:
            continue

        print('成功刷新页面', h_info['网址'])
        try:
            h_info['标题'] = info.select('div.house-details > div.house-title > a.houseListTitle')[0].get_text().strip()
            h_info['总价_' + C_DAY] = info.select('div.pro-price')[0].select('strong')[0].get_text()
            h_info['户型'] = info.select('div.details-item')[0].select('span')[0].get_text()
            h_info['面积'] = info.select('div.details-item')[0].select('span')[1].get_text().split('m')[0]
            h_info['层数'] = info.select('div.details-item')[0].select('span')[2].get_text()
            h_info['交房时间'] = info.select('div.details-item')[0].select('span')[3].get_text().split('建造')[0]
            h_info['楼盘名称'] = info.select('div.details-item')[1].select('span.comm-address')[0].get('title').split('\xa0')[0]
            h_info['地理位置'] = info.select('div.details-item')[1].select('span.comm-address')[0].get('title').split('\xa0')[-1]
        except:
            continue
        #插入楼盘信息页
        insert_houseinfo_to_db(h_info)
        #插入网址列表页
        insert_url_to_db({'网址': h_info['网址'], '采集完毕': True, '已过期': False})
        set_house_collect_satus(h_info['网址'])

    return NORMAL_STATE


# 根据具体的网址，解析出房屋的具体信息
def collect_house_urls(page):
    print('分析网页：', page)
    pagestate = parse_house_urls(page)

    # 找到最后一页了
    if pagestate == LAST_PAGE_STATE:
        print(page, '已经达到最大页码了')
        set_total_collect_satus({'采集状态行': True, '列表是否完整': True, '采集日期': C_DAY})
        return
    elif pagestate == FORBIDDEN_STATE:
        print(page, '需要验证!')
        collect_house_urls(page)
        return


# 解析页面的信息
def parse_house_info(url):
    delaytime = random.randint(1, 3)
    time.sleep(delaytime)
    wb_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')

    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return None, pagestate
    elif pagestate == FORBIDDEN_STATE:
        parse_house_info(url)
    info_dict = {0: '楼盘名称', 1: '地理位置', 2: '交房时间', 3: '住宅类型', 4: '户型', 5: '面积', 6: '方向', 7: '层数', 8: '单价', 9: '首付',
                 10: '月供', 11: '装修程度'}

    house_info = soup.select(houseinfo_selector)
    info_record = {}
    for index, info in enumerate(house_info):
        clear_text = info.text.replace('\t', '').replace('\n', '').replace('\ue003', '').strip()
        info_record[info_dict[index]] = clear_text
    try:
        info_record['网址'] = url
        info_record['单价'] = info_record['单价'].split(' ')[0]
        info_record['面积'] = info_record['面积'].split('平方米')[0]

        clear_text = soup.select('#content > div.clearfix.title-guarantee > h3')[0].text.replace('\t', '').replace('\n',
                                                                                                                   '').strip()
    except:
        print('异常网址:', url)
        return None, PAGE_GONE_STATE

    fabu_date = \
        soup.select('#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > h4 > span.house-encode')[
            0].text
    re_fabu_date = re.search(r'(\d+)年(\d+)月(\d+)日', fabu_date)

    info_record['发布日期'] = re_fabu_date.group(0)
    info_record['标题'] = clear_text
    total_price = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.basic-info.clearfix > span.light.info-tag > em')[0].text
    info_record['总价' + '_' + C_DAY] = total_price
    info = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-desc > div > div')
    if len(info) >= 2:
        # clear_text = info[0].text.replace('\n', '').strip()
        # info_record['核心卖点'+'_'+ C_DAY] = clear_text
        #
        # clear_text = info[1].text.replace('\n', '').strip()
        # info_record['业主心态'+'_'+ C_DAY] = clear_text
        return info_record, NORMAL_STATE
    else:
        return None, PAGE_GONE_STATE


# 获取指定的楼盘的信息：价格，位置，开盘时间 等
def collect_house_info(url):
    if check_if_page_collected(url):
        return
    info_record, pagestate = parse_house_info(url)
    # 页面消失了，可能是下架了，也可能是卖出了，需要把该页面设置为完成状态
    if pagestate == PAGE_GONE_STATE:
        set_house_collect_satus(url, expired=True)
        return
    # 没有获取成功，不更新状态，以便下一次重新采集
    elif info_record == None:
        return
    insert_houseinfo_to_db(info_record)
    set_house_collect_satus(url)


def collect_house_urls_entry():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    last_cnt = url_list.find({'采集完毕':True}).count()
    wangzhi = {url['网址'] for url in url_list.find({'采集完毕':True})}
    while (True):
        lp_page_list = [url_address_format.format(str(i)) for i in range(1, 30)]
        pool = Pool(processes=20)
        print('创建5个进程，开始采集列表页')
        pool.map(collect_house_urls, lp_page_list)
        pool.close()
        pool.join()
        print('上次列表数{}，本次列表数{}'.format(last_cnt, url_list.find({'采集完毕':True}).count()))

        if url_list.find({'采集完毕':True}).count() - last_cnt < 100:
            break
        last_cnt = url_list.find({'采集完毕':True}).count()

    set_total_collect_satus({'采集状态行': True, '列表是否完整': True, '采集日期': C_DAY})
    print('获取网址列表成功')


def get_collect_house_list():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']

    url_list_para = {rec['网址'] for rec in url_list.find()}
    print('从分页显示中爬取的二手房屋个数：', len(url_list_para))

    lp_list = house['楼盘信息页']
    for lp in lp_list.find():
        if '网址' in lp.keys():
            if lp['网址'] not in url_list_para:
                url_list_para.add(lp['网址'])
                url_list.insert_one({'网址': lp['网址'], '采集完毕': False})

    url_list_para -= {rec['网址'] for rec in url_list.find() if rec['采集完毕'] == True or rec['已过期'] == True}
    print('加上上次记录的二手房屋个数后，变成：', len(url_list_para))
    return url_list_para

remain_num = [0, 0, 0, 0, 0]
def collect_progress_task():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    i = 0
    while True:
        cnt = url_list.find({'采集完毕': False, '已过期': False}).count()
        remain_num[i % len(remain_num)] = cnt
        i += 1
        if i > 5 and sum(remain_num) / len(remain_num) == remain_num[0]:
            print('采集陷入停滞状态')
            print('\a')
        print('待采集房屋数量:{}'.format(cnt))
        time.sleep(10)
    pass


def main():
    c_flg = s_flg = a_flg = r_flg = False
    c_flg = True

    if (len(sys.argv) > 1):
        c_flg = '-collect' in sys.argv[1:]
        s_flg = '-save' in sys.argv[1:]
        a_flg = '-analyze' in sys.argv[1:]
        r_flg = '-restart' in sys.argv[1:]

    print('采集开始时间：', time.ctime())
    if r_flg == True:
        set_total_collect_satus({'采集状态行': True, '列表是否完整': False, '采集日期': C_DAY})
    if c_flg:
        t = threading.Thread(target=collect_progress_task)
        t.setDaemon(True)  # don't hang on exit
        t.start()

        print('开始收集列表页')
        collect_house_urls_entry()
        print('列表页收集完毕')



    if s_flg:
        print('开始保存数据')
        export_db_to_file()
        print('保存数据完毕')

    if a_flg:
        print('开始分析数据')
        house_analyze()
        print('分析数据结束')
    print('采集结束时间：', time.ctime())


if __name__ == '__main__':
    main()
