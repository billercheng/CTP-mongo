import pandas as pd
from datetime import *
from sqlalchemy import *
import pymongo
import numpy as np

def dfInsertMongo(df, con, index = True):
    if index:
        df = df.reset_index(drop = False)
    listTemp = []
    for i in range(df.shape[0]):
        dictTemp = df.iloc[i].to_dict()
        dictTemp = insertDbChg(dictTemp)
        listTemp.append(dictTemp)
    con.insert_many(listTemp)

def insertDbChg(dict):  # 主要用于更改数据类型
    for each in dict.keys():
        if isinstance(dict[each], np.int64):
            dict[each] = int(dict[each])
        elif isinstance(dict[each], np.float64):
            dict[each] = float(dict[each])
        if isinstance(dict[each], float):
            dict[each] = round(dict[each], 4)
        # elif isinstance(dict[each], pd._libs.tslib.Timestamp):
        #     dict[each] = dict[each].strftime("%Y-%m-%d %H:%M:%S")
    return dict

if __name__ == '__main__':
    listFreq = [1]
    # listFreq = [1, 5]
    dictFreqDatabase = {}
    dictDiff = {}  # 建立DIFF数据库连接
    # mongodb 数据库
    mon = pymongo.MongoClient("mongodb://localhost:27017/")
    dictMon = {}

    dfFreqDatabase = pd.read_excel("RD files\\公共参数.xlsx", sheetname="数据库名称表", index_col='分钟数')
    for freq in listFreq:
        dictFreqDatabase[freq] = dfFreqDatabase["数据库名称"][freq]
        print(dictFreqDatabase[freq])
        dictDiff[freq] = create_engine('mysql+pymysql://root:rd008@{}:3306/{}?charset=utf8'
                                       .format(dfFreqDatabase["数据库位置"][freq], dictFreqDatabase[freq])).connect()
        # mongodb 数据库连接， 创建数据库
        dictMon[freq] = mon['cta{}_trade'.format(freq)]

    # 获取品种
    dfGoodsTab = pd.read_excel("RD files\\公共参数.xlsx", sheetname="品种信息")
    listGoodsCode = dfGoodsTab['品种代码'].tolist()
    listGoodsName = dfGoodsTab['品种名称'].tolist()

    # 删除数据
    # startTime = datetime(2019, 9, 22, 16)
    startTime = datetime(2019, 1, 1, 8)
    for freq in listFreq:

        if freq == 1:
            con = dictMon[1]
            for goodsName in listGoodsName:
                listTables = ['{}_调整时刻表'.format(goodsName)]
                for eachTable in listTables:
                    # 删除数据操作
                    # 读取dictDiff 数据
                    if eachTable[-3:] == '调整表':
                        table = con[eachTable]
                        table.delete_many({'trade_time': {'$lte': startTime}})
                        df = pd.read_sql("select * from {} where trade_time > '{}'".format(goodsName + '_调整表', startTime), dictDiff[freq])
                        df = df.drop(['id'], axis=1)
                        dfInsertMongo(df, table, index = False)
                        # df.to_sql(goodsName + '_调整表', con, if_exists='append', index=False, schema='cta{}_trade'.format(freq))
                    if eachTable[-5:] == '调整时刻表':
                        table = con[eachTable]
                        table.drop()
                        df = pd.read_sql("select * from {}".format(goodsName + '_调整时刻表', startTime), dictDiff[freq])
                        df = df.drop(['id'], axis=1)
                        df['adjdate'] = pd.to_datetime(df['adjdate'])
                        dfInsertMongo(df, table, index=False)
        else:
            con = dictMon[freq]
            for goodsName in listGoodsName:
                listTables = [goodsName + '_调整表',
                              goodsName + '_均值表',
                              goodsName + '_重叠度表']
                for eachTable in listTables:
                    table = con[eachTable]
                    table.delete_many({'trade_time': {'$lte': startTime}})

                    # 读取dictDiff 数据
                    if eachTable[-3:] == '调整表':
                        df = pd.read_sql("select * from {} where trade_time > '{}'".format(goodsName, startTime), dictDiff[freq])
                        df['close'] = df['close_price']
                        df['oi'] = 0
                        df = df.drop(['id', 'close_price', 'heyue'], axis=1)
                        print(df.columns)
                        print(df)
                        dfInsertMongo(df, table, index=False)
                        # df.to_sql(goodsName + '_调整表', con, if_exists='append', index=False, schema='cta{}_trade'.format(freq))
                    if eachTable[-3:] == '均值表':
                        df = pd.read_sql("select * from {} where trade_time > '{}'".format(goodsName + '_均值表', startTime), dictDiff[freq])
                        df = df.drop(['id'], axis=1)
                        df['open'] = 0
                        print(df.columns)
                        dfInsertMongo(df, table, index=False)
                        # df.to_sql(goodsName + '_均值表', con, if_exists='append', index=False, schema='cta{}_trade'.format(freq))
                    if eachTable[-4:] == '重叠度表':
                        df = pd.read_sql("select * from {} where trade_time > '{}'".format(goodsName + '_重叠度表', startTime), dictDiff[freq])
                        df = df.drop(['id'], axis=1)
                        df['open'] = 0
                        print(df.columns)
                        dfInsertMongo(df, table, index=False)
                        # df.to_sql(goodsName + '_重叠度表', con, if_exists='append', index=False, schema='cta{}_trade'.format(freq))
                # 读取dictDiff数据