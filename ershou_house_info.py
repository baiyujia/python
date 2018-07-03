# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pymongo
import time
import re
from pandas import Series,DataFrame
import pandas as pd
from multiprocessing import Pool
import random
from pachong_status import *

headers = {
    'cookies': 'ctid=31; aQQ_ajkguid=D0174100-3E84-3050-1537-SX0626220205; sessid=F458D7EC-B0CE-6BA4-C930-SX0626220205; isp=true; twe=2; 58tj_uuid=2f50f7d6-4abf-4c60-8eae-cbaf641f9580; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1530021729; als=0; ajk_member_captcha=38a0011b908a1c16fca40584e66cf3ab; lp_lt_ut=a34ae0a5041a1ac2ab5058b3ff39ac1d; lps=http%3A%2F%2Fxa.anjuke.com%2Fsale%2Fp1%2F%7C; init_refer=; new_uv=7; _ga=GA1.2.466653724.1530276981; _gid=GA1.2.612205714.1530276981; browse_comm_ids=398617; new_session=0; propertys=kxqo1b-pb35gy_; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1530277715; _gat=1; __xsptplusUT_8=1; __xsptplus8=8.5.1530276981.1530277735.10%234%7C%7C%7C%7C%7C%23%23LW_EYiniJbsOE0diRvFfQzQG3HUoWAyX%23',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

proxy_list = [
    "http://210.38.1.145:8080",
    "http://101.71.32.154:80",
    "http://101.71.86.30:8080",
    "http://101.71.86.17:80",
    "http://106.14.51.145:8118",
    "http://210.38.1.138:8080",
    "http://122.226.183.147:80",
    "http://115.159.50.52:80",
    "http://91.73.131.254:8080",
    "http://159.203.0.155:3128",
    "http://101.71.86.29:8080",
    "http://218.92.145.42:8080",
    "http://61.136.163.245:8103",
    "http://172.87.132.189:8080",
    "http://212.47.252.49:3128",
    "http://222.255.122.58:3128",
    "http://82.137.250.213:8080",
    "http://50.59.162.26:8118",
    "http://187.87.77.76:3128",
    "http://64.173.224.142:9991",
    "http://31.182.52.156:3129",
    "http://144.217.49.109:80",
    "http://210.38.1.132:8080",
    "http://210.38.1.134:8081",
    "http://124.238.235.135:81",
    "http://103.76.203.22:53281",
    "http://165.138.225.250:8080",
    "http://183.224.12.13:80",
    "http://80.107.117.210:3128",
    "http://101.71.85.180:8080",
    "http://158.69.87.247:3128",
    "http://36.42.32.29:80",
    "http://91.223.64.179:80",
    "http://111.13.7.119:80",
]

db_house = 'house'

url_address_format = 'https://xa.anjuke.com/sale/p{}/'

houseinfo_selector = '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-wrap > div > div dd'

def insert_houseinfo_to_db(info_record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']
    db_record = lp_info.find_one({'标题': info_record['标题']})
    if db_record == None:
        lp_info.insert_one(info_record)
        print('获取房屋:{}'.format(info_record['标题']))
    else:
        lp_info.update_one({'标题': info_record['标题']}, {'$set': {'总价' + '_' + C_DAY: info_record['总价' + '_' + C_DAY],'网址':info_record['网址']}})
        print('更新房屋:{}'.format(info_record['标题']))

#根据页面解析出网址列表
def parse_house_urls(page):
    delaytime = random.randint(1,3)
    time.sleep(delaytime)
    proxy = random.choice(proxy_list)
    proxies = {'http': proxy}
    wb_data = requests.get(page,headers=headers,proxies=proxies)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    
    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return None,pagestate
    
    infos = soup.select('#houselist-mod-new > li.list-item > div.house-details > div.house-title > a.houseListTitle')
    return [info.get('href').split('?')[0] for info in infos], NORMAL_STATE

# 根据具体的网址，解析出房屋的具体信息
def collect_house_urls(page):
    house_url_list, pagestate = parse_house_urls(page)

    #找到最后一页了
    if pagestate == LAST_PAGE_STATE:
        print(page, '已经达到最大页码了')
        set_total_collect_satus({'采集状态行': True,'列表是否完整': True,'采集日期':C_DAY})
        return
    elif pagestate == FORBIDDEN_STATE:
        print(page,'需要验证!')
        return

    # 插入楼盘网址
    for url in house_url_list:
        insert_url_to_db({'网址': url,  '采集完毕':False})
        print('分析网址：',url)

#解析页面的信息
def parse_house_info(url):
    delaytime = random.randint(1,3)
    time.sleep(delaytime)
    proxy = random.choice(proxy_list)
    proxies = {'http': proxy}
    wb_data = requests.get(url,headers=headers,proxies=proxies)
    soup = BeautifulSoup(wb_data.text, 'lxml')

    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return None, pagestate

    info_dict={0:'楼盘名称', 1:'地理位置',2:'交房时间',3:'住宅类型',4:'户型',5:'面积',6:'方向',7:'层数',8:'单价',9:'首付',10:'月供',11:'装修程度'}

    house_info = soup.select(houseinfo_selector)
    info_record={}
    for index,info in enumerate(house_info):
        clear_text = info.text.replace('\t', '').replace('\n', '').replace('\ue003', '').strip()
        info_record[info_dict[index]] = clear_text
    info_record['网址'] = url

    try:
        clear_text = soup.select('#content > div.clearfix.title-guarantee > h3')[0].text.replace('\t', '').replace('\n', '').strip()
    except:
        print('异常网址:', url)
        raise
    info_record['标题'] = clear_text
    total_price = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.basic-info.clearfix > span.light.info-tag > em')[0].text
    info_record['总价' + '_' + C_DAY] = total_price
    return info_record,NORMAL_STATE

# 获取指定的楼盘的信息：价格，位置，开盘时间 等
def collect_house_info(url):

    if check_if_page_collected(url):
        return
    info_record ,pagestate = parse_house_info(url)

    # 页面消失了，可能是下架了，也可能是卖出了，需要把该页面设置为完成状态
    if pagestate == PAGE_GONE_STATE:
        set_house_collect_satus(url)
        return
    # 没有获取成功，不更新状态，以便下一次重新采集
    elif info_record == None:
        return
    insert_houseinfo_to_db(info_record)
    set_house_collect_satus(url)

def collect_house_urls_entry():
    status = get_total_collect_satus()
    if status.find_one()['列表是否完整'] == True:
        return

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_page_list = [url_address_format.format(str(i)) for i in range(1, 50)]
    pool = Pool(processes=20)
    pool.map(collect_house_urls, lp_page_list)
    pool.close()
    pool.join()

    set_total_collect_satus( {'采集状态行': True, '列表是否完整': True, '采集日期': C_DAY})
    print('获取网址列表成功')
# 如果今天没有获取到数据，则填充空白数据
def fix_nulldata():

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    lp_info = house['楼盘信息页']
    i = 0

    for lp in lp_info.find():
        if '总价' + '_' + C_DAY not in lp.keys():
            lp_info.update_one({'网址':lp['网址']},{'$set':{'总价' + '_' + C_DAY:'NA'}})
            i = i + 1
    print('由于数据获取失败，填充空白价格的个数:{}'.format(str(i)))

def get_collect_house_list():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list_para = {rec['网址'] for rec in url_list.find()}
    print('从分页显示中爬取的二手房屋个数：',len(url_list_para))

    lp_list = house['楼盘信息页']
    for lp in lp_list.find():
        if '网址' in lp.keys():
            if lp['网址'] not in url_list_para:
                url_list_para.add(lp['网址'])
                url_list.insert_one({'网址': lp['网址'], '采集完毕': False } )

    print('加上上次记录的二手房屋个数后，变成：',len(url_list_para))
    return url_list_para

#启动多线程进行数据的采集
def collect_house_info_entry():
    url_list_para = get_collect_house_list()

    pool = Pool(processes=20)
    pool.map(collect_house_info, url_list_para)
    pool.close()
    pool.join()
    fix_nulldata()

def export_db_to_file():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']
    print('开始导出数据库文件到csv文件！')
    df = pd.DataFrame(columns=lp_info.find()[0].keys())
    for info in lp_info.find():
        pd_data = DataFrame.from_dict(info, orient='index').T
        df = df.append(pd_data, ignore_index=True,sort=True)
    df.to_csv('./house' + C_DAY + '.csv', sep=',', encoding='utf-8')
    print('导出完毕！')
def house_analyze():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    addrlist={None}
    lp_info = house['楼盘信息页']

    #价格波动房源



    for lp in lp_info.find():
        key_list = lp.keys()
        fj_key_list = []
        for key in key_list:
            if '总价' in key:
                fj_key_list.insert(0, key)

        fj_price_set = set()
        for fj_key in fj_key_list:
            if lp[fj_key] == 'NA':
                if len(fj_price_set) > 1:
                    fj_price_set.add(lp[fj_key])
            else:
                fj_price_set.add(lp[fj_key])
        if len(fj_price_set) > 1:
            str = lp['楼盘名称'] + '----' + lp['单价'] + '----' + lp['标题']
            for fj_key in fj_key_list:
                str += '-->  '+ lp[fj_key]
            print(str)
    #for lp in lp_info.find():

        #更新记录的样例
        # if '总价' in lp.keys():
        #
        #     lp['总价_2018_06_30'] = lp['总价_2018_07_01']
        #     del lp['总价_2018_07_01']
        #     lp_info.replace_one({'网址':lp['网址']},lp)
        # if '总价_201871' in lp.keys() and '总价_2018630' in lp.keys():
        #     if lp['总价_201871'] != lp['总价_2018630']:
        #         print(lp['标题'], lp['总价_2018630'],lp['总价_201871'])

        # if '标题' in lp.keys():
        #     if lp['标题'] in addrlist:
        #         print('网址{} 标题{}' .format(lp['标题'], lp['标题']))
        #     else:
        #         addrlist.add(lp['标题'])

def main():
    collect_house_urls_entry()
    collect_house_info_entry()
    house_analyze()
    export_db_to_file()

if __name__ == '__main__':
    main()