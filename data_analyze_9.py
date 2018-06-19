# pandas 数据源下载地址：https://video.mugglecode.com/data_pd.zip，下载压缩包后解压即可
# -*- coding: utf-8 -*-

"""
    明确任务：比较咖啡厅菜单各饮品类型的产品数量，平均热量
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

datafile_path = './house_data.csv'

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
    data_df.dropna()
    print('丢弃无效数据后的行数： {}' .format(data_df.count()))
    return data_df

def save_and_show_results(data_df):
    """
        结果展示
    """
    sns.boxplot(x='bedrooms',y='price',data=data_df)
    # plt.hist(data_df[data_df['bedrooms'] == 5]['price'],bins=30)

    plt.tight_layout()
    plt.show()

    sns.jointplot(x='bedrooms',y='price',data=data_df)
    plt.tight_layout()
    plt.show()

    corr_data = data_df.corr()
    sns.heatmap(corr_data,annot=True)
    plt.tight_layout()
    plt.show()


def main():
    """
        主函数
    """
    # 数据获取
    data_df = collect_data()

    # 查看数据信息
    inspect_data(data_df)

    # 数据分析
    data_df = analyze_data(data_df)

    # 结果展示
    save_and_show_results(data_df)


if __name__ == '__main__':
    main()
