# -*- coding: utf-8 -*-

import pandas as pd
import time as tm
import numpy as np
from subprocess import PIPE, Popen
from pyhive import hive
import logging

# 日志设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S', filename='/data/public/recommend/logs/em.log',
                    filemode='w')


class EmtropyMethod:
    def __init__(self, index, positive, negative, row_name):
        if len(index) != len(row_name):
            raise Exception('数据指标行数与行名称数不符')
            logging.error("数据指标行数与行名称数不符")
        if sorted(index.columns) != sorted(positive + negative):
            raise Exception('正项指标加负向指标不等于数据指标的条目数')
            logging.error("正项指标加负向指标不等于数据指标的条目数")

        self.index = index.copy().astype('float64')
        self.positive = positive
        self.negative = negative
        self.row_name = row_name

    # 归一化处理
    def uniform(self):
        uniform_mat = self.index.copy()
        min_index = {column: min(uniform_mat[column]) for column in uniform_mat.columns}
        max_index = {column: max(uniform_mat[column]) for column in uniform_mat.columns}
        for i in range(len(uniform_mat)):
            for column in uniform_mat.columns:
                if column in self.negative:
                    uniform_mat[column][i] = (uniform_mat[column][i] - min_index[column]) / (
                            max_index[column] - min_index[column])
                else:
                    uniform_mat[column][i] = (max_index[column] - uniform_mat[column][i]) / (
                            max_index[column] - min_index[column])

        self.uniform_mat = uniform_mat
        logging.info("归一化处理")
        return self.uniform_mat

    # 计算指标比重
    def calc_probability(self):
        try:
            p_mat = self.uniform_mat.copy()
        except AttributeError:
            raise Exception('你还没进行归一化处理，请先调用uniform方法')
        for column in p_mat.columns:
            sigma_x_1_n_j = sum(p_mat[column])
            p_mat[column] = p_mat[column].apply(
                lambda x_i_j: x_i_j / sigma_x_1_n_j if x_i_j / sigma_x_1_n_j != 0 else 1e-6)

        self.p_mat = p_mat
        logging.info("计算指标比重")
        return p_mat

    # 计算熵值
    def calc_emtropy(self):
        try:
            self.p_mat.head(0)
        except AttributeError:
            raise Exception('你还没计算比重，请先调用calc_probability方法')

        e_j = -(1 / np.log(len(self.p_mat) + 1)) * np.array(
            [sum([pij * np.log(pij) for pij in self.p_mat[column]]) for column in self.p_mat.columns])
        ejs = pd.Series(e_j, index=self.p_mat.columns, name='指标的熵值')

        self.emtropy_series = ejs
        logging.info("计算熵值")
        return self.emtropy_series

    # 计算信息熵冗余度
    def calc_emtropy_redundancy(self):
        try:
            self.d_series = 1 - self.emtropy_series
            self.d_series.name = '信息熵冗余度'
        except AttributeError:
            raise Exception('你还没计算信息熵，请先调用calc_emtropy方法')

        logging.info("计算信息熵冗余度")
        return self.d_series

    # 计算权值
    def calc_weight(self):
        self.uniform()
        self.calc_probability()
        self.calc_emtropy()
        self.calc_emtropy_redundancy()
        self.weight = self.d_series / sum(self.d_series)
        self.weight.name = '权值'
        logging.info("计算权值")
        return self.weight

    # 计算评分
    def calc_score(self):
        self.calc_weight()

        self.score = pd.Series(
            [np.dot(np.array(self.index[row:row + 1])[0], np.array(self.weight)) for row in range(len(self.index))]
            , index=self.row_name, name="评分"
        ).sort_values(ascending=False)

        logging.info("计算评分")
        return self.score

    # 数据落盘到HDFS
    def write_csv_hdfs(self):
        self.calc_score()

        currentTime = tm.strftime("%Y%m%d", tm.localtime())
        userproduct = self.score.index.values
        userid = [userproduct[i][0] for i in range(len(userproduct))]
        productid = [userproduct[i][1] for i in range(len(userproduct))]

        self.ups = pd.DataFrame(
            {'user_id': userid, 'paoduct_id': productid, 'score': self.score.values, 'time': currentTime})

        parquetPath = "/data/public/recommend/ups/" + currentTime + "user_product_score.parquet"
        csvPath = "/data/public/recommend/ups/" + currentTime + "user_product_score.csv"

        # 落csv
        self.ups.to_csv(csvPath, index=False, header=False, encoding='utf-8')
        # 落parquet
        self.ups.to_parquet(parquetPath, compression='snappy')

        # 写HDFS
        hdfsPath = "/apps/hive_external/recommend/etd/etd_user_product_score"
        # put parquet into hdfs
        put = Popen(["hdfs", "dfs", "-put", csvPath, hdfsPath], stdin=PIPE, bufsize=-1)
        put.communicate()

        logging.info("数据落盘到HDFS")
        return self.ups


# 加载hive连接信息
def read_hive_conf():
    host = "cdh-s1.sxkj.online"
    PORT = 10000
    name = "songlei"
    password = "s84827409?"
    database = "etd"
    conn = hive.Connection(host=host, port=PORT, username=name, database=database, auth='LDAP', password=password)
    return conn


# 读取数据到DF
def read_data_hive():
    conn = read_hive_conf()
    sql = "select * from temp.user_product_active"

    logging.info("读取数据到DataFrame")
    df = pd.read_sql(sql, conn)
    df = df.dropna().reset_index(drop=True)

    conn.close()
    return df


# 获取用户商品行为数据
def get_user_active():
    conn = read_hive_conf()
    cursor = conn.cursor()
    logging.info("读取用户对商品的行为数据")
    sql1 = "truncate table temp.user_product_active_tmp"
    sql2 = "truncate table temp.user_product_active"
    sql3 = "insert into temp.user_product_active_tmp(user_id,product_id,click_cnt,search_cnt) SELECT distinct_id,productid,count(productid) as click_cnt,sum(isSearch) as search_cnt from etd.etd_behav_bma where productid is not null and distinct_id is not null GROUP BY distinct_id,productid"
    sql4 = "insert into temp.user_product_active_tmp(user_id,product_id,quantity) SELECT cast(uid as string),product_no,quantity from rds.rds_bma_sxyx_sxyp_cart where is_valid = 1 and uid != -2147483648 and uid is not null"
    sql5 = "insert into temp.user_product_active_tmp(user_id,product_id,buy_cnt) SELECT cast(uid as string),product_no as product_id,count(product_no) as buy_cnt from rds.rds_bma_sxyx_sxyp_order where is_valid = 1 and uid != -2147483648 and uid is not null GROUP BY uid,product_no"
    sql6 = "insert into temp.user_product_active SELECT user_id,product_id,case when sum(click_cnt) is null then 0 else sum(click_cnt) end as click_cnt,case when sum(search_cnt) is null then 0 else sum(search_cnt) end as search_cnt,case when sum(quantity) is null then 0 else sum(quantity) end as quantity,case when sum(buy_cnt) is null then 0 else sum(buy_cnt) end as buy_cnt from temp.user_product_active_tmp GROUP BY user_id,product_id"

    cursor.execute(sql1)
    cursor.execute(sql2)
    cursor.execute(sql3)
    cursor.execute(sql4)
    cursor.execute(sql5)
    cursor.execute(sql6)
    cursor.close()
    conn.close()


# load数据到hive
def load_hive():
    currentTime = tm.strftime("%Y%m%d", tm.localtime())

    conn = read_hive_conf()
    cursor = conn.cursor()
    sql = "load data inpath'/apps/hive_external/recommend/etd/etd_user_product_score/" + currentTime + "user_product_score.csv' into table etd.user_product_score"

    logging.info("执行load语句")
    cursor.execute(sql)

    logging.info("Success!")
    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_user_active()
    df = read_data_hive()
    indexs = ["click_cnt", "search_cnt", "quantity", "buy_cnt"]
    rownames = ["user_id", "product_id"]
    Positive = indexs
    Negative = []
    index = df[indexs]
    rowname = df[rownames]

    em = EmtropyMethod(index, Negative, Positive, rowname)

    em.write_csv_hdfs()

    load_hive()
