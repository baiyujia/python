# -*- coding: utf-8 -*-

"""
    人工智能课程工具类

    AUTHOR:     博羿
    VERSION:    1.0
    DATE:       2018/02
"""

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import numpy as np
import seaborn as sns


def do_eda_plot_for_iris(iris_data):
    """
        对鸢尾花数据集进行简单的可视化
        参数：
            - iris_data: 鸢尾花数据集
    """
    category_color_dict = {
        'Iris-setosa':      'red',      # 山鸢尾
        'Iris-versicolor':  'blue',     # 变色鸢尾
        'Iris-virginica':   'green'     # 维吉尼亚鸢尾
    }

    fig, axes = plt.subplots(2, 1, figsize=(8, 8))

    for category_name, category_color in category_color_dict.items():

        # 查看数据的萼片长度(SepalLengthCm)和萼片宽度(SepalWidthCm)
        iris_data[iris_data['Species'] == category_name].plot(ax=axes[0], kind='scatter',
                                                              x='SepalLengthCm', y='SepalWidthCm', label=category_name,
                                                              color=category_color)
        # 查看数据的花瓣长度(PetalLengthCm)和花瓣宽度(PetalWidthCm)
        iris_data[iris_data['Species'] == category_name].plot(ax=axes[1], kind='scatter',
                                                              x='PetalLengthCm', y='PetalWidthCm', label=category_name,
                                                              color=category_color)

    axes[0].set_xlabel('Sepal Length')
    axes[0].set_ylabel('Sepal Width')
    axes[0].set_title('Sepal Length vs Sepal Width')

    axes[1].set_xlabel('Petal Length')
    axes[1].set_ylabel('Petal Width')
    axes[1].set_title('Petal Length vs Petal Width')

    plt.tight_layout()
    plt.savefig('./iris_eda.png')
    plt.show()


def do_pair_plot_for_iris(iris_data):
    """
        对鸢尾花数据集的样本特征关系进行可视化
        参数：
            - iris_data: 鸢尾花数据集
    """
    g = sns.pairplot(data=iris_data[['SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm', 'Species']],
                     hue='Species')
    plt.tight_layout()
    plt.show()
    g.savefig('./iris_pairplot.png')


def plot_knn_boundary(knn_model, X, y, fig_title, save_fig):
    """
        绘制二维平面的kNN边界
        参数：
            knn_mode:   训练好的kNN模型
            X:          数据集特征
            y:          数据集标签
            fig_title:  图像名称
            save_fig:   保存图像的路径
    """
    h = .02  # step size in the mesh

    # Create color maps
    cmap_light = ListedColormap(['#FFAAAA', '#AAFFAA', '#AAAAFF'])
    cmap_bold = ListedColormap(['#FF0000', '#00FF00', '#0000FF'])

    # point in the mesh [x_min, x_max]x[y_min, y_max].
    x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
    y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, h),
                         np.arange(y_min, y_max, h))
    Z = knn_model.predict(np.c_[xx.ravel(), yy.ravel()])

    # Put the result into a color plot
    Z = Z.reshape(xx.shape)
    plt.figure()
    plt.pcolormesh(xx, yy, Z, cmap=cmap_light)

    # Plot also the training points
    plt.scatter(X[:, 0], X[:, 1], c=y, cmap=cmap_bold,
                edgecolor='k', s=20)
    plt.xlim(xx.min(), xx.max())
    plt.ylim(yy.min(), yy.max())
    plt.title(fig_title)

    plt.savefig(save_fig)

    plt.show()


def plot_feat_and_price(house_data):
    """
        绘制每列特征与房价的关系
        参数：
            -house_data: 房屋价格数据集
    """
    feat_cols = ['bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 'sqft_above', 'sqft_basement']
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    for i, feat_col in enumerate(feat_cols):
        house_data[[feat_col, 'price']].plot.scatter(x=feat_col, y='price', alpha=0.5,
                                                     ax=axes[int(i / 3), i - 3 * int(i / 3)])
    plt.tight_layout()
    plt.savefig('./house_feat.png')
    plt.show()


def plot_fitting_line(linear_reg_model, X, y, fig_title, save_fig):
    """
        绘制线性拟合曲线
        参数：
            linear_reg_model:   训练好的线性回归模型
            X:                  数据集特征
            y:                  数据集标签
            fig_title:          图像名称
            save_fig:           保存图像的路径
    """
    # 线性回归模型的系数
    coef = linear_reg_model.coef_

    # 线性回归模型的截距
    intercept = linear_reg_model.intercept_

    # 绘制样本点
    plt.scatter(X, y, alpha=0.5)

    # 绘制拟合线
    plt.plot(X, X * coef + intercept, c='red')

    plt.title(fig_title)
    plt.savefig(save_fig)
    plt.show()
