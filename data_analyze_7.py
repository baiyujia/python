# pandas 数据源下载地址：https://video.mugglecode.com/data_pd.zip，下载压缩包后解压即可
# -*- coding: utf-8 -*-

"""
    明确任务：比较咖啡厅菜单各饮品类型的产品数量，平均热量
"""
import os
import pandas as pd
import matplotlib.pyplot as plt

datafile_path = './data_pd/Beijing_PM.csv'

# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)


def collect_data():
    """
        数据获取
    """
    data_df = pd.read_csv(datafile_path)
    return data_df


def inspect_data(data_df):
    """
        查看数据
    """
    print('数据一共有{}行，{}列'.format(data_df.shape[0], data_df.shape[1]))

    print('数据预览：')
    print(data_df.head())

    print('数据基本信息：')
    print(data_df.info())

    print('数据统计信息：')
    print(data_df.describe())


def analyze_data(data_df):
    """
        数据分析
    """
    year_col = data_df['year']
    year_categories = year_col.unique()
    print('年份：')
    print(year_categories)


    year_grouped = data_df.groupby('year')

    return year_grouped['PM_China','PM_US'].mean()


def save_and_show_results(year_grouped_mean):
    """
        结果展示
    """

    year_grouped_mean.plot(kind='bar')
    plt.title('Category Count')
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'year_grouped_mean.png'))
    year_grouped_mean.to_csv(os.path.join(output_path, 'year_grouped_mean.csv'))




def main():
    """
        主函数
    """
    # 数据获取
    data_df = collect_data()

    # 查看数据信息
    inspect_data(data_df)

    # 数据分析
    year_grouped = analyze_data(data_df)

    # 结果展示
    save_and_show_results(year_grouped)


if __name__ == '__main__':
    main()
