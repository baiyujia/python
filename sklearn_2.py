# 人工智能数据源下载地址：https://video.mugglecode.com/data_ai.zip，下载压缩包后解压即可（数据源与上节课相同）
# -*- coding: utf-8 -*-

"""
    任务：房屋价格预测
"""
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

import ai_utils

DATA_FILE = './diabetes.csv'

# 使用的特征列
FEAT_COLS = ['SEX', 'AGE', 'BMI','BP','S1','S2','S3','S4','S5','S6']


def main():
    """
        主函数
    """
    healthy_data = pd.read_csv(DATA_FILE)
    #ai_utils.plot_feat_and_price(house_data)

    X = healthy_data[FEAT_COLS].values
    y = healthy_data['Y'].values

    # 分割数据集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1/9, random_state=10)

    # 建立线性回归模型
    linear_reg_model = LinearRegression()
    # 模型训练
    linear_reg_model.fit(X_train, y_train)
    # 验证模型
    r2_score = linear_reg_model.score(X_test, y_test)
    print('模型的R2值', r2_score)

    # 单个样本房价预测
    i = 10
    single_test_feat = X_test[i, :]
    y_true = y_test[i]
    y_pred = linear_reg_model.predict([single_test_feat])
    print('样本特征:', single_test_feat)
    print('真实价格：{}，预测价格：{}'.format(y_true, y_pred))


if __name__ == '__main__':
    main()
