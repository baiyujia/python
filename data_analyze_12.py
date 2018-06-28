# pandas 数据源下载地址：https://video.mugglecode.com/data_pd.zip，下载压缩包后解压即可（数据源与上节课相同）
# -*- coding: utf-8 -*-

"""
    明确任务：
        统计不同手机操作系统的每月流量使用情况
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 用户及其使用的手机数据文件
pm_datafile_path = './pm1.csv'



# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)


def collect_data():
    """
        数据获取
    """
    pm_df = pd.read_csv(pm_datafile_path)

    return pm_df


def process_data(pm_df):
    """
        数据处理
    """

    # 合并数据集
    pm_df.dropna()
    pm_df['Timestamp'] =pd.to_datetime(pm_df['Timestamp'])
    pm_df.set_index('Timestamp',inplace=True)
    pm_df_d = pm_df.resample('D').mean()
    return pm_df_d


def analyze_data(merged_df):
    """
        数据分析
    """
    #均值
    merged_df['m3'] = merged_df.rolling(window=3)['PM'].mean()
    merged_df['m5'] = merged_df.rolling(window=5)['PM'].mean()
    merged_df['m7'] = merged_df.rolling(window=7)['PM'].mean()
    return merged_df

def save_plot_results(system_usage_ser):
    """
        结果展示
    """


    system_usage_ser.plot(x='Timestamp',y='m3')
    #sns.boxplot(x= 'system',y='monthly_mb', data=system_usage_ser)
    plt.ylabel('Monthly Usage (MB)')
    plt.tight_layout()

    plt.show()


def main():
    """
        主函数
    """
    # 数据收集
    pm_df = collect_data()

    # 数据处理
    merged_df = process_data(pm_df)

    # 数据分析
    system_usage_ser = analyze_data(merged_df)

    # 结果展示
    save_plot_results(system_usage_ser)


if __name__ == '__main__':
    main()
