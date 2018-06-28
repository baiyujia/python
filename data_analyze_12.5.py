# pandas 数据源下载地址：https://video.mugglecode.com/data_pd.zip，下载压缩包后解压即可（数据源与上节课相同）
# -*- coding: utf-8 -*-

"""
    明确任务：
        按年度、地区分析全球幸福报告
"""
import os
import pandas as pd
import matplotlib.pyplot as plt


# 年度全球幸福报告数据文件
report_datafile_path = './data_pd/happiness_report.csv'

# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)


def collect_data():
    """
        数据获取
    """
    data_df = pd.read_csv(report_datafile_path)
    return data_df


def process_data(data_df):
    """
        数据处理
    """
    data_df.dropna(inplace=True)
    data_df.sort_values(['Year', 'Happiness Score'], ascending=[True, False], inplace=True)
    return data_df


def analyze_data(data_df):
    """
        数据分析
    """
    year_region_grouped_results = data_df.groupby(by=['Year', 'Region'])['Happiness Score'].mean()
    year_region_pivot_results = pd.pivot_table(data_df, index='Region', columns='Year',
                                               values=['Happiness Score', 'Economy (GDP per Capita)'],
                                               aggfunc='mean')
    return year_region_grouped_results, year_region_pivot_results


def save_plot_results(year_region_grouped_results, year_region_pivot_results):
    """
        结果展示
    """
    year_region_grouped_results.to_csv(os.path.join(output_path, 'year_region_grouped_results.csv'))
    year_region_pivot_results.to_csv(os.path.join(output_path, 'year_region_pivot_results.csv'))

    year_region_pivot_results['Happiness Score'].plot(kind='bar', title='Happiness Score')
    plt.tight_layout()
    plt.show()

    year_region_pivot_results['Economy (GDP per Capita)'].plot(kind='bar', title='Economy (GDP per Capita)')
    plt.tight_layout()
    plt.show()


def main():
    """
        主函数
    """
    # 数据获取
    data_df = collect_data()

    # 数据处理
    proc_data_df = process_data(data_df)

    # 数据分析
    year_region_grouped_results, year_region_pivot_results = analyze_data(proc_data_df)

    # 结果展示
    save_plot_results(year_region_grouped_results, year_region_pivot_results)


if __name__ == '__main__':
    main()
