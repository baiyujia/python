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
user_device_datafile_path = './data_pd/mobile_data/user_device.csv'

# 用户及其套餐使用情况的数据文件
user_usage_datafile_path = './data_pd/mobile_data/user_usage.csv'


# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)


def collect_data():
    """
        数据获取
    """
    user_device_df = pd.read_csv(user_device_datafile_path)
    user_usage_df = pd.read_csv(user_usage_datafile_path)
    return user_device_df, user_usage_df


def process_data(user_device_df, user_usage_df):
    """
        数据处理
    """
    # 字符串列合并
    user_device_df['platform_version'] = user_device_df['platform_version'].astype('str')
    user_device_df['system'] = user_device_df['platform'].str.cat(user_device_df['platform_version'], sep='_')

    # 合并数据集
    merged_df = pd.merge(user_device_df, user_usage_df, how='inner', on='user_id')

    return merged_df


def analyze_data(merged_df):
    """
        数据分析
    """
    #均值
    system_usage_ser = merged_df.groupby('system')['monthly_mb'].mean()
    #system_usage_ser = merged_df.groupby('system')
    system_usage_ser.sort_values(ascending=False, inplace=True)
    return system_usage_ser


def save_plot_results(system_usage_ser):
    """
        结果展示
    """
    system_usage_ser.to_csv(os.path.join(output_path, 'mobile_system_usage.csv'))

    system_usage_ser.plot(kind='bar', rot=45)
    #sns.boxplot(x= 'system',y='monthly_mb', data=system_usage_ser)
    plt.ylabel('Monthly Usage (MB)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'mobile_system_usage.png'))
    plt.show()


def main():
    """
        主函数
    """
    # 数据收集
    user_device_df, user_usage_df = collect_data()

    # 数据处理
    merged_df = process_data(user_device_df, user_usage_df)

    # 数据分析
    system_usage_ser = analyze_data(merged_df)

    # 结果展示
    save_plot_results(system_usage_ser)


if __name__ == '__main__':
    main()
