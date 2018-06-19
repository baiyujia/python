# 数据源下载地址：https://video.mugglecode.com/data.zip，下载压缩包后解压即可（数据源与上节课相同）
# -*- coding: utf-8 -*-

"""
    明确任务：统计不同用户骑行时间的直方图
"""
import os
import numpy as np
import matplotlib.pyplot as plt

data_path = './data/bikeshare/'
data_filenames = ['2017-q1_trip_history_data.csv', '2017-q2_trip_history_data.csv',
                  '2017-q3_trip_history_data.csv', '2017-q4_trip_history_data.csv']

# 结果保存路径
output_path = './output'
if not os.path.exists(output_path):
    os.makedirs(output_path)

# 直方图参数
hist_range = (0, 180)
n_bins = 12


def collect_and_process_data():
    """
        Step 1+2: 数据获取，数据处理
    """
    year_duration_member_type_list = []
    for data_filename in data_filenames:
        data_file = os.path.join(data_path, data_filename)
        data_arr = np.loadtxt(data_file, delimiter=',', dtype='str', skiprows=1)

        # 去掉双引号
        # 骑行时间
        duration_col = np.core.defchararray.replace(data_arr[:, 0], '"', '')
        duration_col = duration_col.reshape(-1, 1)
        # 用户类型
        member_type_col = np.core.defchararray.replace(data_arr[:, -1], '"', '')
        member_type_col = member_type_col.reshape(-1, 1)
        duration_member_type_arr = np.concatenate([duration_col, member_type_col], axis=1)

        year_duration_member_type_list.append(duration_member_type_arr)

    year_duration_member_type_arr = np.concatenate(year_duration_member_type_list, axis=0)

    member_arr = year_duration_member_type_arr[year_duration_member_type_arr[:, 1] == 'Member']
    casual_arr = year_duration_member_type_arr[year_duration_member_type_arr[:, 1] == 'Casual']

    year_member_duration = member_arr[:, 0].astype('float') / 1000 / 60
    year_casual_duration = casual_arr[:, 0].astype('float') / 1000 / 60

    return year_member_duration, year_casual_duration


def analyze_data(year_member_duration, year_casual_duration):
    """
        Step 3: 数据分析
    """
    m_duration_hist, m_bin_edges = np.histogram(year_member_duration, range=hist_range, bins=n_bins)
    c_duration_hist, c_bin_edges = np.histogram(year_casual_duration, range=hist_range, bins=n_bins)
    print('会员直方图统计信息：{}, 直方图分组边界:{}'.format(m_duration_hist, m_bin_edges))
    print('非会员直方图统计信息：{}, 直方图分组边界:{}'.format(c_duration_hist, c_bin_edges))


def save_and_show_results(year_member_duration, year_casual_duration):
    """
        Step 4: 结果展示
    """
    fig = plt.figure(figsize=(10, 5))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2, sharey=ax1)

    # 会员直方图
    ax1.hist(year_member_duration, range=hist_range, bins=n_bins)
    ax1.set_xticks(range(0, 181, 15))
    ax1.set_title('Member')
    ax1.set_ylabel('Count')

    # 非会员直方图
    ax2.hist(year_casual_duration, range=hist_range, bins=n_bins)
    ax2.set_xticks(range(0, 181, 15))
    ax2.set_title('Casual')
    ax2.set_ylabel('Count')

    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'type_histogram.png'))
    plt.show()


def main():
    """
        主函数
    """
    # Step 1 + 2: 数据获取，数据处理
    year_member_duration, year_casual_duration = collect_and_process_data()

    # Step 3: 数据分析
    analyze_data(year_member_duration, year_casual_duration)

    save_and_show_results(year_member_duration, year_casual_duration)


if __name__ == '__main__':
    main()
