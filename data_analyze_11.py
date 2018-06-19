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
employee_edu_datafile_path = './data_employee/employee_edu.csv'

# 用户及其套餐使用情况的数据文件
employee_info_datafile_path = './data_employee/employee_info.csv'


# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)


def collect_data():
    """
        数据获取
    """
    employee_edu_df = pd.read_csv(employee_edu_datafile_path)
    employee_info_df = pd.read_csv(employee_info_datafile_path)
    return employee_edu_df, employee_info_df


def process_data(employee_edu_df, employee_info_df):
    """
        数据处理
    """

    # 合并数据集
    merged_df = pd.merge(employee_info_df, employee_edu_df, how='inner', on='EmployeeNumber')

    return merged_df


def analyze_data(merged_df):
    """
        数据分析
    """
    #均值

    return merged_df.groupby('EducationField')['MonthlyIncome'].mean()


def save_plot_results(system_usage_ser):
    """
        结果展示
    """


    system_usage_ser.plot(kind='bar',rot=45)
    #sns.boxplot(x= 'system',y='monthly_mb', data=system_usage_ser)
    plt.ylabel('Monthly Usage (MB)')
    plt.tight_layout()

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
