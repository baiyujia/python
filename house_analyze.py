
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

def main():
    data_df = pd.read_csv(PIC_PATH + 'house' + C_DAY + '.csv',index_col='标题',low_memory=False)

    change_index_list = set()
    price_col = [col for col in data_df.columns if '总价' in col]
    for i in range(1,len(price_col)):
        price_change = data_df[(np.isnan(data_df[price_col[i-1]].values) ^ True)
                                  & (np.isnan(data_df[price_col[i]].values) ^ True)
                                  & (data_df[price_col[i-1]].values != data_df[price_col[i]].values)]
        for e in price_change.index:
            change_index_list.add(e)
    df = data_df.loc[change_index_list].filter(regex='总价').sort_values(by=price_col).T

    for i in range(5,len(change_index_list)+1,5):
        df.iloc[:,i-5:i].plot(kind  = 'line',figsize =(8,5),rot =90)
        matplotlib.pyplot.ylabel('总价(万元)',fontproperties=getChineseFont())
        matplotlib.pyplot.title('房价波动图(2018_6_30至'+C_DAY+')',fontproperties=getChineseFont())
        matplotlib.pyplot.xticks(range(0,len(price_col),1),price_col, fontproperties=getChineseFont())
        matplotlib.pyplot.legend(prop=getChineseFont())
        matplotlib.pyplot.tight_layout()
        matplotlib.pyplot.savefig(PIC_PATH + '房价波动图' + str(int(i/10)) + '.png')
        matplotlib.pyplot.close()
    pass
if __name__ == '__main__':
    main()