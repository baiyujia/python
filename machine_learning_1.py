# -*- coding: utf-8 -*-
# 人工智能数据源下载地址：https://video.mugglecode.com/data_ai.zip，下载压缩包后解压即可
# 课程中的小脚本下载地址：https://video.mugglecode.com/ai_utils.py


"""
    任务：鸢尾花识别
"""
import pandas as pd

from scipy.spatial.distance import euclidean
from sklearn.model_selection import train_test_split
import numpy as np

import ai_utils

DATA_FILE = './data_ai/Iris.csv'

SPECIES = ['Iris-setosa',       # 山鸢尾
           'Iris-versicolor',   # 变色鸢尾
           'Iris-virginica'     # 维吉尼亚鸢尾
           ]

# 使用的特征列
FEAT_COLS = ['SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm']


def get_pred_label(test_sample_feat, train_data):
    """
        “近朱者赤” 找最近距离的训练样本，取其标签作为预测样本的标签
    """
    dis_list = []

    for idx, row in train_data.iterrows():
        # 训练样本特征
        print(type(row))
        train_sample_feat = row[FEAT_COLS].values

        # 计算距离
        dis = euclidean(test_sample_feat, train_sample_feat)
        dis_list.append(dis)

    # 最小距离对应的位置
    pos = np.argmin(dis_list)
    pred_label = train_data.iloc[pos]['Species']
    return pred_label


def main():
    """
        主函数
    """
    # 读取数据集
    iris_data = pd.read_csv(DATA_FILE, index_col='Id')

    # EDA
    ai_utils.do_eda_plot_for_iris(iris_data)

    # 划分数据集
    train_data, test_data = train_test_split(iris_data, test_size=1/3, random_state=10)
    print(type(train_data))
    # 预测对的个数
    acc_count = 0

    # 分类器

    for idx, row in test_data.iterrows():
        print(row[FEAT_COLS].values,row)
        # 测试样本特征
        test_sample_feat = row[FEAT_COLS].values

        # 预测值
        pred_label = get_pred_label(test_sample_feat, train_data)

        # 真实值
        true_label = row['Species']
        print('样本{}的真实标签{}，预测标签{}'.format(idx, true_label, pred_label))

        if true_label == pred_label:
            acc_count += 1

    # 准确率
    print(test_data.shape,acc_count)
    accuracy = acc_count / test_data.shape[0]
    print('预测准确率{:.2f}%'.format(accuracy * 100))


if __name__ == '__main__':
    main()
