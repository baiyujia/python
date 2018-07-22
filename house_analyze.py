
import time
import pymongo
import re
from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from multiprocessing import Pool
import random
from matplotlib.font_manager import FontManager, FontProperties
import matplotlib.pyplot as plt
import sys
PIC_PATH = '/Users/zhlsuo/Desktop/房价走势图/'
C_DAY = "%02d_%02d_%02d" % (time.gmtime().tm_year, time.gmtime().tm_mon, time.gmtime().tm_mday)
def getChineseFont():
    return FontProperties(fname='/Library/Fonts/Songti.ttc')

def price_going(data_df):
    regex_str = None
    if (len(sys.argv) > 1):
        regex_str =  sys.argv[1]

    if regex_str != None:
        data_df = data_df.filter(regex=regex_str, axis=0)
    going_up_list = set()
    going_down_list = set()
    price_col = [col for col in data_df.columns if '总价' in col]
    for i in range(1,len(price_col)):
        data_valid = data_df[(np.isnan(data_df[price_col[i-1]].values) ^ True)
                                  & (np.isnan(data_df[price_col[i]].values) ^ True)]
        price_change = data_valid[data_valid[price_col[i-1]].values < data_valid[price_col[i]].values]
        for e in price_change.index:
            going_up_list.add(e)

        price_change = data_valid[data_valid[price_col[i-1]].values > data_valid[price_col[i]].values]
        for e in price_change.index:
            going_down_list.add(e)

    #波动房源个数
    going_updown_list = going_up_list & going_down_list

    going_up_list = going_up_list - going_updown_list
    going_down_list = going_down_list - going_updown_list
    print('上升房源',len(going_up_list),'下降房源',len(going_down_list),'波动房源',len(going_updown_list))

    df_up = data_df.loc[going_up_list].filter(regex='总价').sort_values(by=price_col).T
    df_down = data_df.loc[going_down_list].filter(regex='总价').sort_values(by=price_col).T
    df_updown = data_df.loc[going_updown_list].filter(regex='总价').sort_values(by=price_col).T




    # for i in range(5,df_up.shape[1]+1,5):
    #     df_up.iloc[:,i-5:i].plot(kind  = 'line',figsize =(8,5),rot =90)
    #     matplotlib.pyplot.ylabel('总价(万元)',fontproperties=getChineseFont())
    #     matplotlib.pyplot.title('房价波动图(2018_6_30至'+C_DAY+')',fontproperties=getChineseFont())
    #     matplotlib.pyplot.xticks(range(0,len(price_col),1),price_col, fontproperties=getChineseFont())
    #     matplotlib.pyplot.legend(prop=getChineseFont())
    #     matplotlib.pyplot.tight_layout()
    #     matplotlib.pyplot.savefig(PIC_PATH + '房价波动图' + str(int(i/5)) + '.png')
    #
    #     matplotlib.pyplot.close()

    gr = data_df.groupby(by='楼盘名称')
    top10_lp = gr.count().sort_values(by='面积',ascending = False).iloc[:-10]
    plt.figure(figsize=(16, 9))
    row = 2
    column = 4
    for i, lp_name  in enumerate(top10_lp.index):
        df = data_df[data_df['楼盘名称'] == lp_name]
        lp_price_stat = price_going_by_lp(df)

        plt.subplot(row, column, 1 + i % (row*column))
        plt.tight_layout()
        plt.plot(lp_price_stat['价格走势'])
        my_y_ticks = np.arange(-0.5,4, 0.5)
        plt.yticks(my_y_ticks)
        plt.grid()
        plt.ylabel(lp_name , fontproperties=getChineseFont())
        # plt.xlabel('房价波动图(2018_6_30至' + C_DAY + ')', fontproperties=getChineseFont())

        plt.xticks([0])
        if ((i + 1) % (row*column) == 0):
            plt.savefig(PIC_PATH + '楼盘房价波动图' + C_DAY + str(int(i / (row*column))) + '.png')
            plt.figure(figsize=(16, 10))

def price_stat(data_df):
    regex_str = None
    if (len(sys.argv) > 1):
        regex_str =  sys.argv[1]

    price_col = [col for col in data_df.columns if '总价' in col]
    print('日期        ', '房源数   ', '均值     ', '最大值   ', '最小值', '中位数   ','面积   ')
    for col in price_col:
        df = data_df[np.isnan(data_df[col]) ^ True]
        if regex_str != None:
            df = df.filter(regex=regex_str, axis = 0)

        #过滤超高房价
        if(df[col].max() / df[col].median() > 10):
            df = df[(df[col] < df[col].median() * 2 ) & (df[col] > df[col].median() / 2 )]

        valid_num = df.shape[0]
        print(col.split('总价_')[1], '%8d' % valid_num,
              '%8.3f' % (df[col].mean() / df['面积'].mean()),
              '%8.3f' % (df[col].max() / df['面积'].mean()),
              '%8.3f' % (df[col].min() / df['面积'].mean()),
              '%8.3f' % (df[col].median() / df['面积'].mean()),
              '%8.0f' % df['面积'].mean())

def price_going_by_lp(data_df):

    price_col = [col for col in data_df.columns if '总价' in col]
    # print('日期        ', '房源数   ', '均值     ', '最大值   ', '最小值', '中位数   ','面积   ','单价   ')
    lp_price_stat = {}
    '''
        {楼盘名称: str,    价格走势: list    日期列表: list   当日房源数: list}
    '''
    lp_price_stat['楼盘名称'] = data_df.iloc[0]['楼盘名称']
    lp_price_stat['价格走势'] = []
    lp_price_stat['日期列表'] = []
    lp_price_stat['当日房源'] = []



    last_price = 0
    for i, col in enumerate(price_col):
        if i == 0:
            continue
        df = data_df[np.isnan(data_df[col]) ^ True]
        if df.shape[0] == 0:
            continue
        # 过滤超高房价
        if (df[col].max() / df[col].median() > 10):
            df = df[(df[col] < df[col].median() * 2) & (df[col] > df[col].median() / 2)]

        valid_num = df.shape[0]
        # if (int(df[col].mean()) != last_price and valid_num > 10):
        #     print(col.split('总价_')[1], '%8d' % valid_num,
        #           '%8.3f' % (df[col].mean() / df['面积'].mean()),
        #           '%8.3f' % (df[col].max() / df['面积'].mean()),
        #           '%8.3f' % (df[col].min() / df['面积'].mean()),
        #           '%8.3f' % (df[col].median() / df['面积'].mean()),
        #           '%8.0f' % df['面积'].mean(),
        #           '%8.0f' % df['单价'].mean())
        try:
            last_price = int(df[col].mean())
        except:
            print('异常日期1', col,data_df.iloc[0]['楼盘名称'])
            continue
        try:
            lp_price_stat['价格走势'].append(df[col].mean() / df['面积'].mean())
            lp_price_stat['日期列表'].append(col.split('总价_')[1])
            lp_price_stat['当日房源'].append(valid_num)
        except:
            print('异常日期',col)
            continue

    return lp_price_stat
if __name__ == '__main__':
    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题',low_memory=False)

    price_going(data_df)
    price_stat(data_df)

