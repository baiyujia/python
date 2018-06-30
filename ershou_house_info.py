# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pymongo
import time
import re
from multiprocessing import Pool

headers = {
    'cookies': 'ctid=31; aQQ_ajkguid=D0174100-3E84-3050-1537-SX0626220205; sessid=F458D7EC-B0CE-6BA4-C930-SX0626220205; isp=true; twe=2; 58tj_uuid=2f50f7d6-4abf-4c60-8eae-cbaf641f9580; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1530021729; als=0; ajk_member_captcha=38a0011b908a1c16fca40584e66cf3ab; lp_lt_ut=a34ae0a5041a1ac2ab5058b3ff39ac1d; lps=http%3A%2F%2Fxa.anjuke.com%2Fsale%2Fp1%2F%7C; init_refer=; new_uv=7; _ga=GA1.2.466653724.1530276981; _gid=GA1.2.612205714.1530276981; browse_comm_ids=398617; new_session=0; propertys=kxqo1b-pb35gy_; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1530277715; _gat=1; __xsptplusUT_8=1; __xsptplus8=8.5.1530276981.1530277735.10%234%7C%7C%7C%7C%7C%23%23LW_EYiniJbsOE0diRvFfQzQG3HUoWAyX%23',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

db_house = 'house'

C_DAY = str(time.gmtime().tm_year) + str(time.gmtime().tm_mon) + str(time.gmtime().tm_mday)

url_address_format = 'https://xa.anjuke.com/sale/p{}/'

houseinfo_selector = '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-wrap > div > div dd'

def check_if_page_invalid(soup):
    if '您要浏览的网页可能被删除' in soup.title.text:
        return True
    else:
        return False

#获取数据爬取的状态
def get_pachong_status():
    client = pymongo.MongoClient('localhost', 27017, connect=False)

    house = client[db_house]
    status = house['采集状态页']

    # 没有记录，就插入一条
    if status.count() == 0:
        status.insert_one({'采集状态行': True,'列表是否完整': False,'采集日期':C_DAY})

    # 列表不完整，就删除掉列表
    if status.find_one()['列表是否完整'] == False:
        house.drop_collection('网址列表页')

    # 和上次更新日期不同，就删除列表，同时刷新本次日期
    if status.find_one()['采集日期'] != C_DAY:
        #把url_list中的完成标识设置为false
        house.drop_collection('网址列表页')
        status.remove()
        status.insert_one({'采集状态行': True,'列表是否完整': False,'采集日期':C_DAY})

    return status


def set_pachong_status(newRec):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    status = house['采集状态页']
    status.remove({})
    status.insert_one(newRec)
    return

def set_url_collect_over(url):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list.update_one({'网址': url}, {'$set', {'采集完毕':True}})
    return


def insert_url_to_db(record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list.insert_one(record)


# 获取所有的楼盘的详细信息网址
def get_lp_urls(page):
    time.sleep(1)

    wb_data = requests.get(page,headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    infos = soup.select('#houselist-mod-new > li.list-item > div.house-details > div.house-title > a.houseListTitle')
    # 找不到数据了，说明已经是最后一页了
    if len(infos) == 0:
        print('获取不到有效数据：', page)
        set_pachong_status({'采集状态行': True,'列表是否完整': True,'采集日期':C_DAY})
        return

    # 插入楼盘网址
    for info in infos:
        lp_address = info.get('href').split('?')[0]
        insert_url_to_db({'网址': lp_address,  '采集完毕':False})
        print('房屋网址:',lp_address)

def check_if_page_collected(url):
    # 先看该网址是否已经采集过了
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    lp_info = house['楼盘信息页']

    lp_info_record = url_list.find_one({'网址':url})
    if lp_info_record['采集完毕'] == True :
        #print('已经采集完毕',url)
        return True
    else:
        return False
def update_lp_info(info_record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']
    db_record = lp_info.find_one({'标题': info_record['标题']})
    if db_record == None:
        lp_info.insert_one(info_record)
        # set_pachong_status({'lp_urllist_complete': True, 'last_url_pos': index})
        print('获取房屋:{}'.format(info_record['标题']))
    else:
        lp_info.update_one({'标题': info_record['标题']}, {'$set': {'总价' + '_' + C_DAY: info_record['总价' + '_' + C_DAY]}})
        print('更新房屋:{}'.format(info_record['标题']))


    print('更新采集状态完毕')

# 获取指定的楼盘的信息：价格，位置，开盘时间 等
def get_lp_info(url):
    if check_if_page_collected(url):
        return

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list.update_one({'网址': url}, {'$set': {'采集完毕':True}})

    print('采集：', url)
    time.sleep(1)
    wb_data = requests.get(url,headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    if check_if_page_invalid(soup):
        print('页面失效',url)
        return


    info_dict={0:'楼盘名称', 1:'地理位置',2:'交房时间',3:'住宅类型',4:'户型',5:'面积',6:'方向',7:'层数',8:'单价',9:'首付',10:'月供',11:'装修程度'}

    house_info = soup.select(houseinfo_selector)
    info_record={}
    for index,info in enumerate(house_info):
        clear_text = info.text.replace('\t', '').replace('\n', '').replace('\ue003', '').strip()
        info_record[info_dict[index]] = clear_text

    clear_text = soup.select('#content > div.clearfix.title-guarantee > h3')[0].text.replace('\t', '').replace('\n', '').strip()
    info_record['标题'] = clear_text
    total_price = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.basic-info.clearfix > span.light.info-tag > em')[0].text
    info_record['总价' + '_' + C_DAY] = total_price
    update_lp_info(info_record)



    #set_url_collect_over(url)
def get_lp_urls_entry():
    status = get_pachong_status()
    if status.find_one()['列表是否完整'] == True:
        return

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_page_list = [url_address_format.format(str(i)) for i in range(1, 50)]
    pool = Pool(processes=1)
    pool.map(get_lp_urls, lp_page_list)
    pool.close()
    pool.join()

    set_pachong_status( {'采集状态行': True, '列表是否完整': True, '采集日期': C_DAY})
    print('获取网址列表成功')
#启动多线程进行数据的采集
def get_lp_info_entry():

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['网址列表页']
    url_list_para = {rec['网址'] for rec in url_list.find()}
    pool = Pool()
    pool.map(get_lp_info, url_list_para)
    pool.close()
    pool.join()


def main():
    get_lp_urls_entry()
    get_lp_info_entry()

if __name__ == '__main__':
    main()