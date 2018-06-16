# -*- coding: utf-8 -*-

import numpy as np

# 1. 读取csv数据文件
data_arr = np.loadtxt('./temp2.csv', delimiter=',', skiprows=1)

month_list = [1, 2, 3]
for month in month_list:
    # 2. 构造布尔型数组
    # data_arr[:, 0]表示月份列
    month_bool_arr = data_arr[:, 0] == month

    # 3. 使用布尔型数组进行数据过滤
    month_temp_arr = data_arr[month_bool_arr][:, 1]

    # 4. 统计最大值、最小值及平均值
    month_max_temp = np.max(month_temp_arr)
    month_min_temp = np.min(month_temp_arr)
    month_ave_temp = np.mean(month_temp_arr)

    # 输出统计结果
    # :.2f表示保留小数点后2位输出
    print('第{}月，气温最大值={:.2f}，最小值={:.2f}，平均值={:.2f}'.format(month,
                                                           month_max_temp, month_min_temp, month_ave_temp))
