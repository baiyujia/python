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

db_house = 'house'+ str(time.gmtime().tm_year) + str(time.gmtime().tm_mon) + str(time.gmtime().tm_mday)

url_address_format = 'https://xa.anjuke.com/sale/p{}/'

houseinfo_selector = '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-wrap > div > div dd'

def get_pachong_status():
    client = pymongo.MongoClient('localhost', 27017, connect=False)

    house = client[db_house]
    status = house['status']
    # 没有记录，就插入一条
    if status.count() == 0:
        status.insert_one({'lp_urllist_complete': False, 'last_url_pos': 0})
    # 有记录，但是上一次没有获取完毕,需要清空url_list表
    elif status.find_one()['lp_urllist_complete'] == False:
        house.drop_collection('url_list')
    # 有记录，并且也已经有完整的url列表了，则直接返回
    else:
        pass
    return status


def set_pachong_status(status, newRec):
    status.remove({})
    status.insert_one(newRec)
    return


# 获取所有的楼盘的详细信息网址
def get_lp_urls():
    status = get_pachong_status()
    if status.find_one()['lp_urllist_complete'] == True:
        return

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['url_list']

    time.sleep(1)
    lp_page_list = [url_address_format.format(str(i)) for i in range(1, 50)]
    for page in lp_page_list:
        wb_data = requests.get(page,headers=headers)
        soup = BeautifulSoup(wb_data.text, 'lxml')
        infos = soup.select('#houselist-mod-new > li.list-item > div.house-details > div.house-title > a.houseListTitle')
        # 找不到数据了，说明已经是最后一页了
        if len(infos) == 0:
            print('total loupan num:{}'.format(url_list.count()))
            set_pachong_status(status, {'lp_urllist_complete': True, 'last_url_pos': 0})
            return

        # 插入楼盘地址
        for info in infos:
            lp_address = info.get('href').split('?')[0]
            url_list.insert_one({'lp:address': lp_address})
        print('total loupan num:{}'.format(url_list.count()))


# 获取指定的楼盘的信息：价格，位置，开盘时间 等
def get_lp_info(url):
    wb_data = requests.get(url,headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    info_dict={0:'楼盘名称', 1:'地理位置',2:'交房时间',3:'住宅类型',4:'户型',5:'面积',6:'方向',7:'层数',8:'单价',9:'首付',10:'月供',11:'装修程度'}

    house_info = soup.select(houseinfo_selector)
    info_record={}
    for index,info in enumerate(house_info):
        clear_text = info.text.replace('\t', '').replace('\n', '').replace('\ue003', '').strip()
        info_record[info_dict[index]] = clear_text

    clear_text = soup.select('#content > div.clearfix.title-guarantee > h3')[0].text.replace('\t', '').replace('\n', '').strip()
    info_record['标题'] = clear_text
    info_record['总价'] = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.basic-info.clearfix > span.light.info-tag > em')[0].text
    return info_record


def get_lp_info_entry():
    status = get_pachong_status()
    lastpos = status.find_one()['last_url_pos']

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    url_list = house['url_list']
    lp_info = house['lp_info']
    print('last caiji pos:{}'.format(lastpos))
    for index, url in enumerate(url_list.find()):
        if index >= lastpos:
            lp_detail_info = get_lp_info(url['lp:address'])
            if not lp_info.find_one({'标题': lp_detail_info['标题']}):
                lp_info.insert_one(lp_detail_info)
                set_pachong_status(status, {'lp_urllist_complete': True, 'last_url_pos': index})
                print('获取第{}个楼盘:{}'.format(index, lp_detail_info['标题']))


def main():
    get_lp_urls()
    get_lp_info_entry()

if __name__ == '__main__':
    main()