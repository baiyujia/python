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
from matplotlib.font_manager import FontManager, FontProperties
import matplotlib
import sys
import threading
headers = {
    'cookies': 'ctid=31; aQQ_ajkguid=D0174100-3E84-3050-1537-SX0626220205; sessid=F458D7EC-B0CE-6BA4-C930-SX0626220205; isp=true; twe=2; 58tj_uuid=2f50f7d6-4abf-4c60-8eae-cbaf641f9580; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1530021729; als=0; ajk_member_captcha=38a0011b908a1c16fca40584e66cf3ab; lp_lt_ut=a34ae0a5041a1ac2ab5058b3ff39ac1d; lps=http%3A%2F%2Fxa.anjuke.com%2Fsale%2Fp1%2F%7C; init_refer=; new_uv=7; _ga=GA1.2.466653724.1530276981; _gid=GA1.2.612205714.1530276981; browse_comm_ids=398617; new_session=0; propertys=kxqo1b-pb35gy_; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1530277715; _gat=1; __xsptplusUT_8=1; __xsptplus8=8.5.1530276981.1530277735.10%234%7C%7C%7C%7C%7C%23%23LW_EYiniJbsOE0diRvFfQzQG3HUoWAyX%23',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

proxy_list = [
]


db_house = 'house'
PIC_PATH = '/Users/zhlsuo/Desktop/房价走势图/'
url_address_format = 'https://xa.anjuke.com/sale/p{}/'

houseinfo_selector = '#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-wrap > div > div dd'

def insert_houseinfo_to_db(info_record):
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']

    lp_info.update_one({'网址': info_record['网址']}, {'$set': info_record}, upsert=True)
    #db_record = lp_info.find_one({'网址': info_record['网址']})
    # if db_record == None:
    #     lp_info.insert_one(info_record)
    #     # print('获取房屋:{}'.format(info_record['标题']))
    # else:
    #     lp_info.update_one({'网址': info_record['网址']}, {'$set': info_record},upsert=True)
        #print('更新房屋:{}'.format(info_record['标题']))
def getChineseFont():
    return FontProperties(fname='/Library/Fonts/Songti.ttc')
#根据页面解析出网址列表
def parse_house_urls(page):
    delaytime = random.randint(1,3)
    time.sleep(delaytime)
    #proxy = random.choice(proxy_list)
    #proxies = {'http': proxy}
    wb_data = requests.get(page,headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    
    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return None,pagestate
    
    infos = soup.select('#houselist-mod-new > li.list-item > div.house-details > div.house-title > a.houseListTitle')
    return [info.get('href').split('?')[0] for info in infos], NORMAL_STATE

# 根据具体的网址，解析出房屋的具体信息
def collect_house_urls(page):
    print('分析网页：',page)
    house_url_list, pagestate = parse_house_urls(page)

    #找到最后一页了
    if pagestate == LAST_PAGE_STATE:
        print(page, '已经达到最大页码了')
        set_total_collect_satus({'采集状态行': True,'列表是否完整': True,'采集日期':C_DAY})
        return
    elif pagestate == FORBIDDEN_STATE:
        print(page,'需要验证!')
        collect_house_urls(page)
        return

    # 插入楼盘网址
    for url in house_url_list:
        insert_url_to_db({'网址': url,  '采集完毕':False})
        #print('分析网址：',url)

#解析页面的信息
def parse_house_info(url):
    delaytime = random.randint(1,3)
    time.sleep(delaytime)
    #print('采集房屋',url)
    wb_data = requests.get(url,headers=headers)
    soup = BeautifulSoup(wb_data.text, 'lxml')

    pagestate = get_page_state(soup)
    if pagestate != NORMAL_STATE:
        return None, pagestate
    elif pagestate == FORBIDDEN_STATE:
        parse_house_info(url)
    info_dict={0:'楼盘名称', 1:'地理位置',2:'交房时间',3:'住宅类型',4:'户型',5:'面积',6:'方向',7:'层数',8:'单价',9:'首付',10:'月供',11:'装修程度'}

    house_info = soup.select(houseinfo_selector)
    info_record={}
    for index,info in enumerate(house_info):
        clear_text = info.text.replace('\t', '').replace('\n', '').replace('\ue003', '').strip()
        info_record[info_dict[index]] = clear_text
    info_record['网址'] = url
    info_record['单价'] = info_record['单价'].split(' ')[0]
    try:
        clear_text = soup.select('#content > div.clearfix.title-guarantee > h3')[0].text.replace('\t', '').replace('\n', '').strip()
    except:
        print('异常网址:', url)
        raise

    fabu_date = \
    soup.select('#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > h4 > span.house-encode')[
        0].text
    re_fabu_date = re.search(r'(\d+)年(\d+)月(\d+)日', fabu_date)

    info_record['发布日期'] = re_fabu_date.group(0)
    info_record['标题'] = clear_text
    total_price = soup.select(
        '#content > div.wrapper > div.wrapper-lf.clearfix > div.basic-info.clearfix > span.light.info-tag > em')[0].text
    info_record['总价' + '_' + C_DAY] = total_price
    info = soup.select('#content > div.wrapper > div.wrapper-lf.clearfix > div.houseInfoBox > div > div.houseInfo-desc > div > div')
    clear_text = info[0].text.replace('\n', '').strip()
    info_record['核心卖点'+'_'+ C_DAY] = clear_text

    clear_text = info[1].text.replace('\n', '').strip()
    info_record['业主心态'+'_'+ C_DAY] = clear_text
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
    pool = Pool(processes=5)
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
    url_list_para -= {rec['网址'] for rec in url_list.find() if rec['采集完毕'] == True}
    print('加上上次记录的二手房屋个数后，变成：',len(url_list_para))
    return url_list_para

#启动多线程进行数据的采集
def collect_house_info_entry():

    url_list_para = get_collect_house_list()
    #无效的房屋直接设置为采集完毕
    drop_invalid_house()
    pool = Pool(processes=20)
    pool.map(collect_house_info, url_list_para)
    pool.close()
    pool.join()
    #fix_nulldata()

def export_db_to_file():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]

    lp_info = house['楼盘信息页']
    print('开始导出数据库文件到csv文件！')
    df = pd.DataFrame(columns=lp_info.find()[0].keys())
    for info in lp_info.find():
        pd_data = DataFrame.from_dict(info, orient='index').T
        df = df.append(pd_data, ignore_index=True,sort=True)
    df.to_csv(PIC_PATH + 'house' + C_DAY + '.csv', sep=',')
    print('导出完毕！')

def plot_price_going():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    addrlist={None}
    lp_info = house['楼盘信息页']

    #价格波动房源

    bodong_lp_list=[]
    f = open(PIC_PATH + '房屋走势.txt','w')
    for lp in lp_info.find():
        # 找出所有的价格标签
        fj_key_list = [key for key in lp.keys() if '总价' in key]
        #找出所有的价格
        fj_price_set = {lp[fj_key] for fj_key in fj_key_list if lp[fj_key] != 'NA'}
        #如果价格的数量大于1，说明价格有波动
        if len(fj_price_set) > 1:
            bodong_lp_list.append(lp)
            str_out = lp['楼盘名称'] + '----' + lp['单价'] + '----' + lp['标题']
            for fj_key in fj_key_list:
                str_out += '-->  '+ lp[fj_key]
            f.write(str_out)
            f.write('\n')
    f.close()
    biaoi_list = [lp['标题'] for lp in bodong_lp_list]

    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题')

    df = data_df.loc[biaoi_list].filter(regex='总价').T
    for i in range(5,len(bodong_lp_list)+1,5):
        #plt.figure(1,figsize=(16,10),dpi=300)

        df.iloc[:, i-5:i].plot(rot=90,figsize=(8,5),grid=True)

        matplotlib.pyplot.ylabel('总价(万元)',fontproperties=getChineseFont())
        matplotlib.pyplot.title('房价波动图(2018_6_30至'+C_DAY+')',fontproperties=getChineseFont())
        matplotlib.pyplot.xticks(range(0,15,1), fontproperties=getChineseFont())
        matplotlib.pyplot.legend(prop=getChineseFont())
        matplotlib.pyplot.tight_layout()
        matplotlib.pyplot.savefig(PIC_PATH + '房价波动图' + str(int(i/10)) + '.png')

def pos_parse_fun(para):
    para['区域'] = para['地理位置'].split('－')[0]
    para['子区域'] = para['地理位置'].split('－')[1]
    para['街道'] = para['地理位置'].split('－')[2]
    return para

def plot_group_fenbu():
    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题')
    data_df['区域'] = '-'
    data_df['子区域'] = '-'
    data_df['街道'] = '-'

    data_df = data_df.apply(pos_parse_fun,axis=1)
    data_df.groupby('区域').mean().sort_values(by='单价')['单价'].plot(kind='bar',grid=True,figsize=(20,12),rot=70)

    matplotlib.pyplot.ylabel('单价(元)', fontproperties=getChineseFont())
    matplotlib.pyplot.title('区域房屋均价', fontproperties=getChineseFont())
    matplotlib.pyplot.xticks(fontproperties=getChineseFont())
    matplotlib.pyplot.legend(prop=getChineseFont())
    matplotlib.pyplot.tight_layout()
    matplotlib.pyplot.savefig(PIC_PATH + '区域分布.png')

def plot_groupby_name(name):
    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题')

    data_df.filter(regex=name, axis=0).filter(regex='单价').plot(kind='bar',grid=True,figsize=(20,12),rot=90)
    matplotlib.pyplot.ylabel('单价(元)', fontproperties=getChineseFont())
    matplotlib.pyplot.title('房屋售价图', fontproperties=getChineseFont())
    matplotlib.pyplot.xticks(fontproperties=getChineseFont())
    matplotlib.pyplot.legend(prop=getChineseFont())
    matplotlib.pyplot.tight_layout()
    matplotlib.pyplot.savefig(PIC_PATH + name + '.png')

#累计3天无统计信息，则不再采集
def drop_invalid_house():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    addrlist={None}
    lp_info = house['楼盘信息页']
    discard_cnt = 0
    for lp in lp_info.find():
        na_cnt=0
        for k,v in lp.items():
            if '总价' in k and v == 'NA':
                na_cnt += 1
        if na_cnt > 3:
            set_house_collect_satus(lp['网址'])
            discard_cnt += 1
    print('无效房源总计：',discard_cnt)

#由于标题变动，合并相同网址的价格信息
def house_hebing():
    client = pymongo.MongoClient('localhost', 27017, connect=False)
    house = client[db_house]
    addrlist={None}
    lp_info = house['楼盘信息页']
    wz_list=set()
    repeat_wz=set()
    for lp in lp_info.find():
        if lp['网址'] in wz_list:
            print('重复：',lp['标题'])
            repeat_wz.add(lp['网址'])
        wz_list.add(lp['网址'])
    print(len(repeat_wz))
    for i in range(len(repeat_wz)):
        j=0
        repeat_bt=set()
        for info in lp_info.find({'网址': repeat_wz.pop()}):
            if j == 0:
                tmp = info
            else:
                repeat_bt.add(info['标题'])
                for k,v in  tmp.items():
                    if '总价' in k and v == 'NA':
                        if k in info.keys() and info[k]!= 'NA':
                            tmp[k] = info[k]
                            # print(k,v,info[k])
            j+=1

        lp_info.update_one({'标题': tmp['标题']}, {'$set': tmp})

        for t in range(len(repeat_bt)):
            bt = repeat_bt.pop()
            lp_info.delete_one({'标题':bt})
            print('删除标题：',bt)


        print('-'*50)
        for k,v in tmp.items():
            if '总价' in k:
                print(k,v)


#分析波动房源，并且打印出来
def house_analyze():
    plot_price_going()
    plot_group_fenbu()
    plot_groupby_name('紫薇*田园*都市')


def collect_progress_task():

    client   = pymongo.MongoClient('localhost', 27017, connect=False)
    house    = client[db_house]
    url_list = house['网址列表页']
    while True:
        print('获取房屋进度:{}/{}' .format(url_list.find({'采集完毕': True}).count() , url_list.find().count()))
        time.sleep(10)
    pass


def main():
    c_flg = s_flg = a_flg = r_flg = False
    a_flg = True
    if (len(sys.argv) > 1):
        c_flg = '-collect' in sys.argv[1:]
        s_flg = '-save' in sys.argv[1:]
        a_flg = '-analyze' in sys.argv[1:]
        r_flg = '-restart' in sys.argv[1:]


    print('采集开始时间：', time.ctime())
    if r_flg == True:
        set_total_collect_satus({'采集状态行': True, '列表是否完整': False, '采集日期': C_DAY})
    if c_flg:
        print('开始收集列表页')
        collect_house_urls_entry()
        print('列表页收集完毕')

        t = threading.Thread(target=collect_progress_task)
        t.setDaemon(True)  # don't hang on exit
        t.start()

        print('开始收集详细页')
        collect_house_info_entry()
        print('详细页收集完毕')

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