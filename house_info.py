# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pymongo
import time
import re
from multiprocessing import Pool

headers={
     'cookies': 'isp=true; ctid=31; aQQ_ajkguid=D0174100-3E84-3050-1537-SX0626220205; sessid=F458D7EC-B0CE-6BA4-C930-SX0626220205; isp=true; twe=2; 58tj_uuid=2f50f7d6-4abf-4c60-8eae-cbaf641f9580; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1530021729; als=0; new_uv=2; ajk_member_captcha=38a0011b908a1c16fca40584e66cf3ab; ved_loupans=242866%3A242953%3A252964%3A426484; lp_lt_ut=efb2032f53cc373954da0962c60fc7c2; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1530027665; __xsptplus8=8.2.1530024412.1530027665.12%234%7C%7C%7C%7C%7C%23%23YbR6hkdsOxbIMdmglrBT1ibmXjWJyJ3b%23',
     'user-agent': '/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
}

url_address_format = 'https://xa.fang.anjuke.com/loupan/all/p{}/'

def get_pachong_status():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client['house']
    status = house['status']
    #没有记录，就插入一条
    if status.count() == 0:
        status.insert_one({'lp_urllist_complete':False,'last_url_pos':0})
    #有记录，但是上一次没有获取完毕,需要清空url_list表
    elif status.find_one()['lp_urllist_complete'] == False:
        house.drop_collection('url_list')
    #有记录，并且也已经有完整的url列表了，则直接返回
    else:
        pass
    return status

def set_pachong_status(status, newRec):
    status.remove({})
    status.insert_one(newRec)
    return

#获取所有的楼盘的详细信息网址
def get_lp_urls():
    status = get_pachong_status()
    if status.find_one()['lp_urllist_complete'] == True:
        return

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client['house']
    url_list = house['url_list']

    time.sleep(1)
    lp_page_list = [url_address_format.format(str(i)) for i in range(1,50)]
    for page  in  lp_page_list:
        wb_data=requests.get(page)
        soup = BeautifulSoup(wb_data.text, 'lxml')
        infos = soup.select('div.list-contents > div.list-results > div.key-list > div.item-mod > div.infos')
        #找不到数据了，说明已经是最后一页了
        if len(infos) == 0:
            print('total loupan num:{}'.format(url_list.count()))
            set_pachong_status(status, {'lp_urllist_complete':True,'last_url_pos':0})
            return

        #插入楼盘地址
        for info in infos:
            lp_address = info.select('a.lp-name')[0].get('href')
            url_list.insert_one({'lp:address':lp_address})
        print('total loupan num:{}'.format(url_list.count()))

#获取指定的楼盘的信息：价格，位置，开盘时间 等
def get_lp_info(url):
    wb_data = requests.get(url)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    lp_name = soup.select('#j-triggerlayer')[0].text
    price = soup.select('#container > div.main-detail.mod > div.basic-details > div.basic-parms-wrap > dl > dd.price')[
        0].text
    if '待定' in price:
        price = '待定'
    price_around = soup.select(
        '#container > div.main-detail.mod > div.basic-details > div.basic-parms-wrap > dl > dd.around-price > span')
    if len(price_around) == 0:
        price_around = '无'
    else:
        price_around = price_around[0].text

    addr = soup.select(
        '#container > div.main-detail.mod > div.basic-details > div.basic-parms-wrap > dl > dd > span.lpAddr-text')
    if len(addr) == 0:
        addr = '无'
    else:
        addr = addr[0].text

    up_time = soup.select(
        '#container > div.main-detail.mod > div.basic-details > div.basic-parms-wrap > dl > dd > span.lpAddr-text')
    if len(up_time) == 0:
        up_time = '无'
    else:
        up_time = up_time[0].text

    return {'lpname':lp_name, 'price':price, 'price_around':price_around, 'addr': addr.strip()}


def get_lp_info_entry():
    status = get_pachong_status()
    lastpos = status.find_one()['last_url_pos']

    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client['house']
    url_list = house['url_list']
    lp_info = house['lp_info']
    print('last caiji pos:{}' .format(lastpos))
    for index, url in enumerate(url_list.find()):
        if index >= lastpos:
            lp_detail_info = get_lp_info(url['lp:address'])
            if not lp_info.find_one({'lpname':lp_detail_info['lpname']}):
                lp_info.insert_one(lp_detail_info)
                set_pachong_status(status, {'lp_urllist_complete': True, 'last_url_pos': index})
                print('获取第{}个楼盘:{}' .format(index,lp_detail_info['lpname']))

def main():
    get_lp_urls()
    get_lp_info_entry()



if __name__ == '__main__':
    main()