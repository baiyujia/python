
import time
import pymongo
import re
from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from multiprocessing import Pool
import random
from matplotlib.font_manager import FontManager, FontProperties
import matplotlib
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




    for i in range(5,df_up.shape[1]+1,5):
        df_up.iloc[:,i-5:i].plot(kind  = 'line',figsize =(8,5),rot =90)
        matplotlib.pyplot.ylabel('总价(万元)',fontproperties=getChineseFont())
        matplotlib.pyplot.title('房价波动图(2018_6_30至'+C_DAY+')',fontproperties=getChineseFont())
        matplotlib.pyplot.xticks(range(0,len(price_col),1),price_col, fontproperties=getChineseFont())
        matplotlib.pyplot.legend(prop=getChineseFont())
        matplotlib.pyplot.tight_layout()
        matplotlib.pyplot.savefig(PIC_PATH + '房价波动图' + str(int(i/5)) + '.png')

        matplotlib.pyplot.close()

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
              '%8.3f' % (df[col].mean()),
              '%8.0f' % (df[col].max() ),
              '%8.0f' % (df[col].min() ),
              '%8.0f' % (df[col].median()),
              '%8.0f' % df['面积'].mean())

def price_going_by_lp(data_df):

    price_col = [col for col in data_df.columns if '总价' in col]
    print('日期        ', '房源数   ', '均值     ', '最大值   ', '最小值', '中位数   ','面积   ','单价   ')
    lp_price_stat = []
    last_price = 0
    for col in price_col:
        df = data_df[np.isnan(data_df[col]) ^ True]

        # 过滤超高房价
        if (df[col].max() / df[col].median() > 10):
            df = df[(df[col] < df[col].median() * 2) & (df[col] > df[col].median() / 2)]

        valid_num = df.shape[0]
        if (int(df[col].mean()) != last_price and valid_num > 10):
            print(col.split('总价_')[1], '%8d' % valid_num,
                  '%8.3f' % (df[col].mean()),
                  '%8.0f' % (df[col].max() ),
                  '%8.0f' % (df[col].min() ),
                  '%8.0f' % (df[col].median()),
                  '%8.0f' % df['面积'].mean(),
                  '%8.0f' % df['单价'].mean())

        last_price = int(df[col].mean())

        lp_price_stat.append({'楼盘':df['楼盘名称'],'单价':(df['单价'].mean()),'日期':col.split('总价_')[1]})
        #print(df['单价'].mean())

    return lp_price_stat
if __name__ == '__main__':
    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题',low_memory=False)

    price_going(data_df)
    price_stat(data_df)
    gr = data_df.groupby(by='楼盘名称')
    top10_lp = gr.count().sort_values(by='面积',ascending = False).iloc[20:30]
    for lp_name  in top10_lp.index:
        df = data_df[data_df['楼盘名称'] == lp_name]
        print('统计走势:',lp_name,df.shape)
        lp_price_stat = price_going_by_lp(df)
